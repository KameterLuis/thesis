import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("TAOSTATS_API_KEY")

API_BASE = "https://api.taostats.io/api"

def debug_snapshot(df_latest: pd.DataFrame, label: str):
    if df_latest.empty:
        print(f"[{label}] df_latest is EMPTY")
        return

    n_rows = len(df_latest)
    n_hotkeys = df_latest["hotkey"].nunique()
    n_coldkeys = df_latest["coldkey"].nunique()

    n_active_rows = (df_latest["active"] == True).sum()
    n_inactive_rows = (df_latest["active"] == False).sum()

    print(f"[{label}] rows pulled: {n_rows}")
    print(f"[{label}] unique hotkeys: {n_hotkeys}")
    print(f"[{label}] unique coldkeys: {n_coldkeys}")
    print(f"[{label}] active rows: {n_active_rows}, inactive rows: {n_inactive_rows}")

    # top 5 coldkeys by reward_rate_rao before aggregation, just to eyeball concentration
    tmp = (
        df_latest.groupby("coldkey", dropna=False)["reward_rate_rao"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
    )
    print(f"[{label}] top coldkeys by raw reward_rate_rao:")
    print(tmp)
    print("-----")


class Taostats:
    def __init__(self, rpm_limit: int = 5):
        self.s = requests.Session()
        # If your key needs to go in "x-api-key" instead of "Authorization", swap this line:
        # self.s.headers.update({"x-api-key": API_KEY})
        self.s.headers.update({"Authorization": f"{API_KEY}"})
        self.rpm_limit = rpm_limit
        self._last = 0.0

    def _throttle(self):
        gap = 60.0 / max(1, self.rpm_limit)
        now = time.time()
        if now - self._last < gap:
            time.sleep(gap - (now - self._last))
        self._last = time.time()

    def get(self, path: str, **params):
        self._throttle()
        url = f"{API_BASE}{path}"
        r = self.s.get(url, params=params, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"{r.status_code} {r.text[:300]}")
        return r.json()

    def fetch_snapshot_before(
        self,
        netuid: int,
        center_iso: str,
        lookback_hours: int = 24,
        limit: int = 200,
    ) -> pd.DataFrame:
        """
        Approximate the subnet state at time T by:
        - Looking back N hours from T
        - Pulling /metagraph/history/v1 in that whole range
        - Keeping the *latest* row per hotkey (newest block_number/timestamp)

        This gives us a near-complete roster at T, not just whoever updated
        in a tiny 5-minute slice.
        """

        center_ts = pd.to_datetime(center_iso)
        start_ts = int((center_ts - pd.Timedelta(hours=lookback_hours)).timestamp())
        end_ts   = int(center_ts.timestamp())

        rows_all = []
        page = 1

        while True:
            payload = self.get(
                "/metagraph/history/v1",
                netuid=netuid,
                timestamp_start=start_ts,
                timestamp_end=end_ts,
                order="timestamp_desc",  # newest first
                page=page,
                limit=limit,
            )

            data_rows = payload.get("data") or []
            if not data_rows:
                break

            for r in data_rows:
                # coldkey = economic owner
                c = r.get("coldkey", {})
                if isinstance(c, dict):
                    coldkey_ss58 = c.get("ss58") or c.get("hex")
                else:
                    coldkey_ss58 = c

                # hotkey = worker
                h = r.get("hotkey", {})
                if isinstance(h, dict):
                    hotkey_ss58 = h.get("ss58") or h.get("hex")
                else:
                    hotkey_ss58 = h

                # parse stake info
                alpha_raw = r.get("alpha_stake")
                root_as_alpha_raw = r.get("root_stake_as_alpha")
                stake_legacy_raw = r.get("stake")  # before dTAO, this is real

                alpha_val = float(alpha_raw) if alpha_raw is not None else 0.0
                root_as_alpha_val = float(root_as_alpha_raw) if root_as_alpha_raw is not None else 0.0
                legacy_val = float(stake_legacy_raw) if stake_legacy_raw is not None else 0.0

                # dTAO-style stake if available, otherwise legacy stake
                effective_stake_rao = alpha_val + root_as_alpha_val
                if effective_stake_rao == 0.0 and legacy_val > 0.0:
                    effective_stake_rao = legacy_val

                # parse reward flow
                daily_reward_raw = r.get("daily_reward")
                emission_raw = r.get("emission")

                if daily_reward_raw is not None:
                    reward_rate_rao = float(daily_reward_raw)
                elif emission_raw is not None:
                    reward_rate_rao = float(emission_raw)
                else:
                    reward_rate_rao = 0.0

                rows_all.append({
                    "netuid": netuid,
                    "timestamp": r.get("timestamp"),
                    "block_number": r.get("block_number"),
                    "hotkey": hotkey_ss58,
                    "coldkey": coldkey_ss58,
                    "effective_stake_rao": effective_stake_rao,
                    "reward_rate_rao": reward_rate_rao,
                    "active": r.get("active", True),
                })

            # pagination
            pagination = payload.get("pagination", {})
            total_pages = pagination.get("total_pages")
            if total_pages is None:
                page += 1
            else:
                if page >= total_pages:
                    break
                page += 1

        df = pd.DataFrame(rows_all)
        if df.empty:
            return df

        # Deduplicate: newest record per hotkey
        df["block_number"] = pd.to_numeric(df["block_number"], errors="coerce")
        df["ts_parsed"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # sort so that the first we keep is the newest (desc by block/timestamp)
        df_sorted = df.sort_values(
            ["hotkey", "block_number", "ts_parsed"],
            ascending=[True, False, False],
        )
        df_latest = df_sorted.drop_duplicates(subset=["hotkey"], keep="first").reset_index(drop=True)

        return df_latest


def aggregate_by_coldkey(df_latest: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate hotkeys -> coldkeys.
    Compute total stake and total reward per coldkey,
    then normalize to get stake_share and reward_share.
    """
    if df_latest.empty:
        return pd.DataFrame(columns=[
            "coldkey", "stake_total", "reward_total",
            "stake_share", "reward_share"
        ])

    # OLD (too strict):
    # df_active = df_latest[df_latest["active"] == True].copy()

    # NEW (better for economic capture):
    df_active = df_latest.copy()

    agg = (
        df_active.groupby("coldkey", dropna=False)
        .agg(
            stake_total=("effective_stake_rao", "sum"),
            reward_total=("reward_rate_rao", "sum"),
        )
        .reset_index()
    )

    total_stake = agg["stake_total"].sum()
    total_reward = agg["reward_total"].sum()

    agg["stake_share"] = agg["stake_total"] / total_stake if total_stake > 0 else 0.0
    agg["reward_share"] = agg["reward_total"] / total_reward if total_reward > 0 else 0.0

    agg = agg.sort_values("reward_share", ascending=False).reset_index(drop=True)
    return agg


if __name__ == "__main__":
    api = Taostats(rpm_limit=5)

    netuid = 64  # choose the subnet you're analyzing

    # timestamps you care about:
    # T_PRE  = "2025-02-01T23:59:00Z"  # ~before dTAO
    # T_POST = "2025-08-01T23:59:00Z"  # ~after dTAO
    T_PRE  = "2025-02-01T23:59:00Z"
    T_POST = "2025-08-01T23:59:00Z"

    # 1. PRE
    df_pre_latest = api.fetch_snapshot_before(
        netuid=netuid,
        center_iso=T_PRE,
        lookback_hours=24  # we'll change this next
    )
    debug_snapshot(df_pre_latest, "PRE")

    pre_agg = aggregate_by_coldkey(df_pre_latest)
    post_agg.to_csv(f"bittensor_subnet{netuid}_pre.csv", index=False)

    df_post_latest = api.fetch_snapshot_before(
        netuid=netuid,
        center_iso=T_POST,
        lookback_hours=24
    )
    debug_snapshot(df_post_latest, "POST")

    post_agg = aggregate_by_coldkey(df_post_latest)
    post_agg.to_csv(f"bittensor_subnet{netuid}_post.csv", index=False)
    print("post snapshot:", len(post_agg), "coldkeys")
