import time
from datetime import datetime, timedelta, timezone

import bittensor as bt
import pandas as pd
from tqdm import tqdm

# --- CONFIGURATION ---
OUTPUT_FILE = "bittensor_history_official.parquet"

# TIMEFRAME: Aug 24, 2024 to Aug 24, 2025
START_DATE = pd.Timestamp("2024-08-24", tz="UTC")
END_DATE = pd.Timestamp("2024-08-25", tz="UTC")

# TARGET SUBNETS (Top 6 Pareto)
# SN0 (Root) is mandatory. The others are usually the largest.
TARGET_SUBNETS = [0, 1, 2, 3, 4, 5]

# BLOCK TIMING (Standard Finney Network)
BLOCK_TIME_SECONDS = 12
# Reference: Block 3.6M was roughly Aug 19, 2024
REF_BLOCK = 3600000
REF_TIME = pd.Timestamp("2024-08-19T00:00:00", tz="UTC")


def get_block_by_date(target_date):
    """Calculates the block number for a specific date."""
    delta = target_date - REF_TIME
    delta_seconds = delta.total_seconds()
    blocks_diff = int(delta_seconds / BLOCK_TIME_SECONDS)
    return max(0, REF_BLOCK + blocks_diff)


def main():
    print("--- Connecting to Bittensor Network (Finney Archive) ---")
    # This connects you to the public blockchain nodes
    try:
        subtensor = bt.subtensor(network="finney")
        current_block = subtensor.block
        print(f"✅ Connected! Current Chain Block: {current_block}")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("Try running: pip install bittensor --upgrade")
        return

    # 1. Generate Weekly Schedule
    weeks = []
    current_date = START_DATE
    while current_date <= END_DATE:
        weeks.append(current_date)
        current_date += timedelta(weeks=1)

    print(f"Plan: Fetching {len(weeks)} snapshots for {len(TARGET_SUBNETS)} subnets.")
    print("Estimated Time: ~30-60 mins (depends on internet speed)")

    all_data = []

    # 2. Iterate through time
    for week_date in tqdm(weeks, desc="Processing Weeks"):
        target_block = get_block_by_date(week_date)

        # Safety: Don't ask for blocks that haven't happened yet
        if target_block > current_block:
            continue

        for netuid in TARGET_SUBNETS:
            try:
                # --- THE MAGIC ---
                # lite=True gets only Stake/Emission (fast).
                # We do NOT download the weights (slow).
                metagraph = subtensor.metagraph(
                    netuid=netuid, block=target_block, lite=True
                )

                # Extract vectors
                # Note: bittensor returns these as Torch tensors or similar objects,
                # we convert to numpy/list immediately to save memory.

                # Filter for active entities immediately to save RAM
                # We use the 'coldkeys' list to map UIDs to Entities

                df_subnet = pd.DataFrame(
                    {
                        "coldkey": metagraph.coldkeys,
                        "stake": metagraph.S.tolist(),  # Stake (Tao)
                        "emission": metagraph.E.tolist(),  # Emission (Rao)
                    }
                )

                # Add Metadata
                df_subnet["subnet"] = netuid
                df_subnet["block"] = target_block
                df_subnet["week"] = week_date

                # Filter: Keep only non-zero rows (Entities that matter)
                df_active = df_subnet[
                    (df_subnet["stake"] > 0) | (df_subnet["emission"] > 0)
                ].copy()

                if not df_active.empty:
                    all_data.append(df_active)

            except Exception as e:
                # Common Error: "Subnet does not exist at block"
                # This is expected for younger subnets (e.g. SN5 in early 2024)
                # We just skip it.
                pass

    # 3. Aggregation & Save
    if all_data:
        print("Aggregating Raw Data...")
        full_df = pd.concat(all_data, ignore_index=True)

        # Group by Entity (Coldkey) per Week
        # This gives you exactly what you need for the Gini calc
        df_entity = (
            full_df.groupby(["week", "coldkey"])
            .agg(
                {
                    "emission": "sum",  # Total emission across all subnets
                    "stake": "max",  # Max stake (best proxy for total wealth)
                    "subnet": "count",  # Number of subnets they are active in
                }
            )
            .reset_index()
        )

        # Unit Conversion: Emission is usually in Rao (1e-9). Convert to Tao?
        # Let's check magnitude later. Usually output is raw.

        print(f"✅ Success! Captured {len(df_entity)} entity-weeks.")
        df_entity.to_parquet(OUTPUT_FILE, index=False)
        print(f"Saved to {OUTPUT_FILE}")
    else:
        print("❌ No data collected. Check connection.")


if __name__ == "__main__":
    main()
