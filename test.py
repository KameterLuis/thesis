import bittensor as bt
import pandas as pd
from datetime import timedelta
from tqdm import tqdm

# --- CONFIGURATION ---
OUTPUT_FILE = "bittensor_history_raw_storage.parquet"

# TIMEFRAME: Aug 24, 2024 to Aug 24, 2025
START_DATE = pd.Timestamp("2024-08-24", tz="UTC")
END_DATE   = pd.Timestamp("2024-08-25", tz="UTC")

# TARGET SUBNETS (Top 6 Pareto)
TARGET_SUBNETS = [0, 1, 2, 3, 4, 5]

# ARCHIVE NODE
# We use the explicit URL to guarantee archive access
ARCHIVE_URL = "wss://archive.chain.opentensor.ai:443"

# BLOCK TIMING
BLOCK_TIME_SECONDS = 12
REF_BLOCK = 3600000 
REF_TIME  = pd.Timestamp("2024-08-19T00:00:00", tz="UTC")

def get_block_by_date(target_date):
    delta = target_date - REF_TIME
    blocks_diff = int(delta.total_seconds() / BLOCK_TIME_SECONDS)
    return max(0, REF_BLOCK + blocks_diff)

def main():
    print(f"--- Connecting to Archive Raw Storage: {ARCHIVE_URL} ---")
    
    try:
        # Initialize Subtensor
        sub = bt.subtensor(network="archive")
        
        # Access the underlying SubstrateInterface (The magic backdoor)
        # In some versions it's .substrate, in others it's .interface or ._substrate
        substrate = getattr(sub, 'substrate', getattr(sub, 'interface', None))
        
        if not substrate:
            raise ValueError("Could not access underlying SubstrateInterface")
            
        current_block = substrate.get_block_header()['header']['number']
        print(f"✅ Connected! Chain Height: {current_block}")
        
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 1. Generate Schedule
    weeks = []
    current_date = START_DATE
    while current_date <= END_DATE:
        weeks.append(current_date)
        current_date += timedelta(weeks=1)

    print(f"Plan: {len(weeks)} snapshots x {len(TARGET_SUBNETS)} subnets.")
    all_data = []

    # 2. Raw Collection Loop
    for week_date in tqdm(weeks, desc="Querying Raw Storage"):
        target_block = get_block_by_date(week_date)
        
        if target_block > current_block:
            continue

        # Get the Block Hash (Required for raw storage queries)
        try:
            block_hash = substrate.get_block_hash(target_block)
        except Exception:
            # If block is missing or error, skip
            continue

        for netuid in TARGET_SUBNETS:
            try:
                # --- THE BYPASS ---
                # Instead of metagraph.sync(), we query the storage map directly.
                # Storage: SubtensorModule -> Neurons(netuid) -> [NeuronInfo...]
                
                # This returns a generator of (storage_key, scale_obj)
                q = substrate.query_map(
                    module='SubtensorModule',
                    storage_function='Neurons',
                    params=[netuid],
                    block_hash=block_hash
                )

                subnet_data = []
                
                for key, neuron_obj in q:
                    # Decode the Scale Object to a Dictionary
                    neuron = neuron_obj.value
                    
                    # Extract fields safely
                    coldkey = neuron.get('coldkey')
                    hotkey = neuron.get('hotkey')
                    emission = int(neuron.get('emission', 0))
                    
                    # Stake handling: In older versions, stake might be a dict {coldkey: amount} 
                    # or a simple int 'total_stake'. We handle both.
                    raw_stake = neuron.get('stake', 0)
                    stake = 0
                    
                    if isinstance(raw_stake, dict):
                        # Sum values if it's a map
                        stake = sum(raw_stake.values())
                    elif isinstance(raw_stake, list):
                        # Sum tuples if it's a list [(coldkey, amount)]
                        stake = sum(item[1] for item in raw_stake)
                    else:
                        # It's just a number
                        stake = int(raw_stake)

                    if stake > 0 or emission > 0:
                        subnet_data.append({
                            "coldkey": coldkey,
                            "hotkey": hotkey,
                            "stake": stake,
                            "emission": emission,
                            "subnet": netuid,
                            "block": target_block,
                            "week": week_date
                        })

                if subnet_data:
                    all_data.append(pd.DataFrame(subnet_data))

            except Exception as e:
                # Common to fail if subnet didn't exist or storage schema changed slightly
                print(f"  Debug: SN{netuid} error at {target_block}: {e}")
                pass

    # 3. Save
    if all_data:
        print("Aggregating Raw Data...")
        full_df = pd.concat(all_data, ignore_index=True)
        
        # Thesis Aggregation: Entity (Coldkey) per Week
        df_entity = full_df.groupby(["week", "coldkey"]).agg({
            "emission": "sum",       
            "stake": "max",          
            "subnet": "count"        
        }).reset_index()
        
        print(f"✅ Success! Captured {len(df_entity)} entity-weeks.")
        df_entity.to_parquet(OUTPUT_FILE, index=False)
        print(f"Saved to {OUTPUT_FILE}")
    else:
        print("❌ No data collected.")

if __name__ == "__main__":
    main()