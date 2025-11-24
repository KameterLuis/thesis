import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# --- CONFIGURATION ---
# We use the SAME input file you already processed
INPUT_FILE = Path("eth_rich_snapshot.json")
# We save to a NEW output file
OUTPUT_FILE = "eth_entity_history_part2.parquet"

# TIMEFRAME 2: Feb 22, 2025 to Aug 24, 2025
# (I corrected 2024 -> 2025 based on your thesis timeline. Change back if needed!)
START_DATE = pd.Timestamp("2025-02-22", tz="UTC")
END_DATE = pd.Timestamp("2025-08-24", tz="UTC")

# ETHEREUM CONSTANTS
GENESIS_TIME = 1606824023
SECONDS_PER_EPOCH = 384


def get_epoch_from_date(date_obj):
    return int((date_obj.timestamp() - GENESIS_TIME) // SECONDS_PER_EPOCH)


def main():
    print(
        f"--- Generating Part 2 Dataset ({START_DATE.date()} - {END_DATE.date()}) ---"
    )

    # 1. Load Data (Fast if you have it on SSD)
    print(f"Loading {INPUT_FILE}...")
    with open(INPUT_FILE, "r") as f:
        raw = json.load(f)

    validators = raw.get("data", []) if isinstance(raw, dict) else raw

    # 2. Parse Metadata
    print("Parsing timestamps...")
    parsed = []
    for v in tqdm(validators):
        val_obj = v.get("validator", v)
        parsed.append(
            {
                "entity_id": val_obj.get("withdrawal_credentials", "unknown"),
                "activation_epoch": int(
                    val_obj.get("activation_epoch", "18446744073709551615")
                ),
                "exit_epoch": int(val_obj.get("exit_epoch", "18446744073709551615")),
            }
        )

    df_vals = pd.DataFrame(parsed)

    # 3. Generate Weekly Snapshots
    print("Time Traveling...")
    all_weekly_stats = []
    current_date = START_DATE

    while current_date <= END_DATE:
        target_epoch = get_epoch_from_date(current_date)

        # Who was active *specifically* in this week?
        # Logic: Activated BEFORE now, and Exited AFTER now (or never)
        active_mask = (df_vals["activation_epoch"] <= target_epoch) & (
            df_vals["exit_epoch"] > target_epoch
        )

        current_active = df_vals[active_mask]

        # Count validators per Entity
        week_stats = (
            current_active.groupby("entity_id")
            .size()
            .reset_index(name="validator_count")
        )
        week_stats["week"] = current_date

        all_weekly_stats.append(week_stats)

        # Advance 1 week
        current_date += timedelta(weeks=1)

    # 4. Save
    final_df = pd.concat(all_weekly_stats, ignore_index=True)
    final_df.to_parquet(OUTPUT_FILE, index=False)

    print(f"\n✅ DONE! Saved Part 2 to {OUTPUT_FILE}")
    print(f"Total Rows: {len(final_df)}")
    print(f"Unique Entities in this period: {final_df['entity_id'].nunique()}")


if __name__ == "__main__":
    main()
