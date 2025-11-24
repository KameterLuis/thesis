import os
import json
import time
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv

import requests
import pandas as pd
import numpy as np
from tqdm import tqdm

load_dotenv()


###############################################################################
# Config
###############################################################################

API_BASE = "https://beaconcha.in/api/v1"
API_KEY  = os.getenv("SF_BEACON_KEY")  # <-- company key
VALIDATORS_JSON = Path("eth-validators-test.json")  # <-- file your coworker gave you

# Restrict analysis window if you want (ISO8601); we will filter by week_start/week_end
WINDOW_START = pd.Timestamp("2025-02-13T00:00:00Z")
WINDOW_END   = pd.Timestamp("2025-08-13T00:00:00Z")

BATCH_SIZE = 100  # per docs: up to 100 validators per call
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

# Beacon Chain constants
# Beacon genesis was 2020-12-01 12:00:23 UTC.
# Each epoch = 32 slots, each slot = 12 seconds => 384 seconds/epoch.
BEACON_GENESIS = pd.Timestamp("2020-12-01T12:00:23Z")
SECONDS_PER_EPOCH = 32 * 12  # 384


###############################################################################
# HELPERS
###############################################################################

def creds_to_entity(creds_hex: str, fallback_index: int) -> str:
    """
    withdrawal_credentials decoding:
    - If first byte is 0x01 or 0x02 (post-Shapella/Pectra style),
      bytes 12..31 of the 32-byte credential are the ETH withdrawal address.
      That's who actually gets paid, i.e. the economic owner / pool.
    - Otherwise (legacy 0x00 style), we can't recover an address, so we just
      treat that validator as its own tiny entity: 'legacy_<index>'.
    """
    raw = bytes.fromhex(creds_hex[2:])
    prefix = raw[0]
    if prefix in (1, 2) and len(raw) == 32:
        addr = "0x" + raw[12:].hex()
        return addr.lower()
    return f"legacy_{fallback_index}"


def load_validators_snapshot(validators_json_path: Path) -> pd.DataFrame:
    """
    validators_json should look like:
    {
      "version": "...",
      "validators": [
        {
          "index": "0",
          "pubkey": "...",
          "withdrawal_credentials": "0x01...",
          "state": "active_ongoing"
        },
        ...
      ]
    }

    Returns a DataFrame:
      validator_index (int)
      entity_id (str)
    """
    with open(validators_json_path, "r") as f:
        raw = json.load(f)

    recs = []
    for v in raw["validators"]:
        idx = int(v["index"])
        entity_id = creds_to_entity(v["withdrawal_credentials"], idx)
        recs.append({
            "validator_index": idx,
            "entity_id": entity_id,
            "withdrawal_credentials": v["withdrawal_credentials"],
            "state": v.get("state"),
        })
    return pd.DataFrame(recs)


def bc_get(path: str, params=None):
    """
    Minimal GET wrapper for beaconcha.in API.
    Adds apikey query param if present.
    """
    if params is None:
        params = {}
    if API_KEY:
        params["apikey"] = API_KEY
    url = f"{API_BASE}{path}"
    r = SESSION.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_balancehistory_batch(indices: list[int],
                               latest_epoch: int) -> pd.DataFrame:
    """
    Hit /validator/{i1,i2,...}/balancehistory with a specific latest_epoch.
    We intentionally DO NOT try to pull long time series here. We only want
    "what does each validator look like around this epoch (i.e. this week)?"

    Empirically:
    - This returns exactly one row per validator in 'indices' for that epoch's
      accounting week, with fields:
        balance
        effectivebalance
        epoch
        validatorindex
        week
        week_start
        week_end
    """
    id_str = ",".join(str(i) for i in indices)

    js = bc_get(
        f"/validator/{id_str}/balancehistory",
        params={
            "latest_epoch": latest_epoch,
            "limit": 1,   # just give me the snapshot row for that epoch
            "offset": 0,
        },
    )

    arr = js.get("data", [])
    if not arr:
        return pd.DataFrame(
            columns=[
                "validator_index", "week", "epoch",
                "week_start", "week_end",
                "balance_gwei", "effectivebalance_gwei",
            ]
        )

    rows = []
    for rec in arr:
        rows.append({
            "validator_index": int(rec["validatorindex"]),
            "week": int(rec["week"]),
            "epoch": int(rec["epoch"]),
            "week_start": pd.to_datetime(rec.get("week_start"), utc=True, errors="coerce"),
            "week_end":   pd.to_datetime(rec.get("week_end"),   utc=True, errors="coerce"),
            "balance_gwei": int(rec["balance"]),
            "effectivebalance_gwei": int(rec["effectivebalance"]),
        })

    return pd.DataFrame(rows)


def ts_to_epoch(ts: pd.Timestamp) -> int:
    """
    Convert a UTC timestamp to a beacon-chain epoch index:
        epoch = floor((ts - BEACON_GENESIS) / 384s)
    """
    # ensure tz-aware UTC
    ts_utc = ts.tz_convert("UTC") if ts.tzinfo is not None else ts.tz_localize("UTC")
    delta_sec = (ts_utc - BEACON_GENESIS).total_seconds()
    if delta_sec < 0:
        return 0
    return int(delta_sec // SECONDS_PER_EPOCH)


###############################################################################
# CORE AGGREGATION LOOP
###############################################################################

def aggregate_over_weeks(validators_df: pd.DataFrame,
                         window_start: pd.Timestamp,
                         window_end: pd.Timestamp,
                         freq: str = "7D") -> pd.DataFrame:
    """
    Main streaming pipeline.

    Steps:
    - Build a list of weekly timestamps in [window_start, window_end] with step 'freq'.
    - Keep a dict prev_balance[validator_index] to compute weekly issuance deltas.
    - For each target week timestamp:
        * Convert it to a beacon-chain epoch
        * For each batch of up to 100 validators:
            - fetch snapshot for that epoch
            - for each validator row:
                -> issuance = max(balance_now - prev_balance[v], 0)
                -> stake   += effectivebalance_gwei
                -> issuance += issuance
                -> update prev_balance[v] = balance_now
        * After finishing all batches for that week:
            - turn per-entity totals into shares
            - append rows to global output list

    Returns a DataFrame of entity-week rows:
      [
        week_chain,
        week_end,
        entity_id,
        issuance_gwei_pos,
        effectivebalance_gwei,
        reward_share,
        stake_share
      ]
    """

    idx_to_entity = dict(
        zip(validators_df["validator_index"], validators_df["entity_id"])
    )
    all_indices = validators_df["validator_index"].tolist()

    # We'll iterate over weekly "target timestamps"
    weekly_targets = pd.date_range(
        start=window_start,
        end=window_end,
        freq=freq,
        tz="UTC"
    )

    prev_balance = {}  # validator_index -> last seen balance_gwei
    out_rows = []

    # Outer loop: each target week timestamp
    for ts_week in tqdm(weekly_targets, desc="Weeks in window"):
        latest_epoch = ts_to_epoch(ts_week)

        # We'll collect entity-level totals just for THIS week
        issuance_entity = defaultdict(int)      # entity_id -> total positive delta
        stake_entity    = defaultdict(int)      # entity_id -> total effectivebalance
        week_end_for_this_ts = None
        chain_week_id = None

        # Inner loop: batches of validators
        for start_i in tqdm(range(0, len(all_indices), BATCH_SIZE),
                            desc="Validator batches",
                            leave=False):
            batch = all_indices[start_i:start_i + BATCH_SIZE]

            hist = fetch_balancehistory_batch(batch, latest_epoch=latest_epoch)
            if hist.empty:
                continue

            # For each validator row returned for this epoch snapshot
            for row in hist.itertuples(index=False):
                vidx = row.validator_index
                ent  = idx_to_entity.get(vidx)
                bal  = row.balance_gwei
                eff  = row.effectivebalance_gwei

                # compute issuance delta vs last week
                prev_bal = prev_balance.get(vidx)
                if prev_bal is None:
                    # first time we see this validator -> treat issuance as 0
                    delta_pos = 0
                else:
                    delta = bal - prev_bal
                    delta_pos = delta if delta > 0 else 0

                issuance_entity[ent] += int(delta_pos)
                stake_entity[ent]    += int(eff)

                # update rolling balance for next week
                prev_balance[vidx] = bal

                # track chain's notion of "which week is this"
                if row.week_end is not None:
                    week_end_for_this_ts = row.week_end
                chain_week_id = row.week

        # After we finish looping over all validators for this ts:
        total_issuance = sum(issuance_entity.values())
        total_stake    = sum(stake_entity.values())

        # convert to shares and store rows
        for ent_id, issuance_val in issuance_entity.items():
            eff_val = stake_entity.get(ent_id, 0)

            reward_share = (issuance_val / total_issuance) if total_issuance > 0 else 0
            stake_share  = (eff_val / total_stake) if total_stake > 0 else 0

            out_rows.append({
                "week_chain": chain_week_id,
                "week_end": week_end_for_this_ts,
                "entity_id": ent_id,
                "issuance_gwei_pos": issuance_val,
                "effectivebalance_gwei": eff_val,
                "reward_share": reward_share,
                "stake_share": stake_share,
            })

    return pd.DataFrame(out_rows)


###############################################################################
# MAIN
###############################################################################

def main():
    validators_df = load_validators_snapshot(VALIDATORS_JSON)

    # For development / testing: sample a tiny subset so you can iterate fast.
    # Comment this out for the full run.
    validators_df = validators_df.sample(n=40, random_state=42).reset_index(drop=True)

    df_entity_weeks = aggregate_over_weeks(
        validators_df,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        freq="7D",  # 7-day cadence; aligns well with beaconcha.in's "week_start/week_end"
    )

    print("Final rows:", len(df_entity_weeks))
    print(df_entity_weeks.head())

    if len(df_entity_weeks):
        df_entity_weeks.to_parquet("eth_entity_weeks.parquet", index=False)
        print("Saved eth_entity_weeks.parquet")


if __name__ == "__main__":
    main()