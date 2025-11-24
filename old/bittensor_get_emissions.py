import time
import os
import math
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
import requests
import pandas as pd
import numpy as np

load_dotenv()

API_KEY = os.getenv('TAOSTATS_API_KEY')

API_BASE = "https://api.taostats.io/api"

class Taostats:
    def __init__(self, rpm_limit: int = 5, header_mode: str = "x-api-key"):
        self.s = requests.Session()
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

    def metagraph_history(self, netuid: int, at_time: str):
        path = "/metagraph/history/v1"

        try:
            return self.get(path, netuid=netuid, at_time=at_time)
        except RuntimeError:
            dt = pd.to_datetime(at_time)
            start = (dt - pd.Timedelta(minutes=5)).isoformat()
            end   = (dt + pd.Timedelta(minutes=5)).isoformat()
            payload = self.get(path, netuid=netuid, start_time=start, end_time=end)

            items = payload.get("data") or payload.get("items") or [payload]
            return items[-1] if items else payload

def _label_from_hotkey(hk):
    if isinstance(hk, dict):
        return hk.get("ss58") or hk.get("hex") or str(hk)
    return str(hk)

def shares_from_pairs(pairs):
    mapped = { _label_from_hotkey(hk): float(val) for hk, val in pairs if float(val) > 0 }
    s = pd.Series(mapped, dtype=float)
    return s / s.sum() if s.sum() > 0 else pd.Series(dtype=float)

def fetch_distribution_snapshot(tapi: Taostats, netuid: int, when_iso: str) -> pd.Series:
    snap = tapi.metagraph_history(netuid=netuid, at_time=when_iso)
    rows = snap.get("data") or snap.get("miners") or snap
    pairs = []
    for r in (rows if isinstance(rows, list) else []):
        hk = r.get("hotkey") or r.get("ss58") or r.get("key") or r.get("uid")
        val = r.get("incentive") or r.get("emission") or r.get("weight")
        if hk is not None and val is not None:
            pairs.append((hk, val))
    return shares_from_pairs(pairs)

api = Taostats()
s = fetch_distribution_snapshot(api, netuid=2, when_iso="2025-06-30T23:59:00Z")
s.to_csv("bittensor_data.csv")
print("n miners:", len(s), "sum:", s.sum(), "top5:")
print(s.sort_values(ascending=False).head())