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


API_BASE = "https://beaconcha.in/api/v1"
API_KEY  = os.getenv("SF_BEACON_KEY") 
VALIDATORS_JSON = Path("eth-validators.json")

# Restrict analysis window if you want (ISO8601); we will filter by week_start/week_end
WINDOW_START = pd.Timestamp("2025-02-13T00:00:00Z")
WINDOW_END   = pd.Timestamp("2025-08-13T00:00:00Z")

BATCH_SIZE = 100           # up to 100 validators per request (per docs)
PER_VALIDATOR_LIMIT = 100  # up to 100 history rows per validator

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

def load_validator_addresses(validators_json_path: Path):
    with open(validators_json_path, "r") as f:
        raw = json.load(f)

    records = []
    for v in raw["validators"]:
        entity_key = v["pubkey"]
        records.append(entity_key)
    return records

def bc_get(path: str, params=None):
    if params is None:
        params = {}
    if API_KEY:
        params["apikey"] = API_KEY
    url = f"{API_BASE}{path}"
    r = SESSION.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_balancehistory_batch(addresses) -> pd.DataFrame:
    """
    Call /validator/{idx1,idx2,...}/balancehistory once for up to 100 validators.
    Response (per docs screenshot):
      {
        "data": [
          {
            "balance": 1,
            "effectivebalance": 1,
            "epoch": 1,
            "validatorindex": 1,
            "week": 1,
            "week_end": "string",
            "week_start": "string"
          }
        ],
        "status": "string"
      }
    """
    addr_string = ",".join(str(i) for i in addresses)

    print(addr_string)

    js = bc_get(f"/validator/{addr_string}/balancehistory")
    arr = js.get("data", [])

    if not arr:
        return pd.DataFrame(columns=[
            "validator_index","week","epoch","week_start","week_end",
            "balance_gwei","effectivebalance_gwei"
        ])

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

    df = pd.DataFrame(rows)
    return df


def main():
    validator_addresses = load_validator_addresses(VALIDATORS_JSON)

    print(len(validator_addresses))

    # OPTIONAL: sampling mode for development
    # validators_df = validators_df.sample(n=40, random_state=42).reset_index(drop=True)

    #batch = fetch_balancehistory_batch(validator_addresses)

    #print(batch)

    #if not df_entity_week.empty:
    #    df_entity_week.to_parquet("eth_entity_week.parquet", index=False)


if __name__ == "__main__":
    main()