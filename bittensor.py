import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
API_KEY = os.getenv("TAOSTATS_API_KEY")  # <--- PASTE YOUR KEY HERE
START_DATE = "2025-02-13"
END_DATE = "2025-08-13" 
SUBNETS = [64] # Add your subnets here
OUTPUT_FILE = "bittensor_sn64_post.parquet"

# API CONSTANTS
BASE_URL = "https://api.taostats.io/api"
HEADERS = {
    "Authorization": API_KEY, 
    "Content-Type": "application/json"
}

def fetch_daily_prices(start_ts, end_ts):
    """
    Fetches prices for ALL subnets in the given window using dtao/pool/history.
    Returns a dict: {netuid: price}
    """
    url = f"{BASE_URL}/dtao/pool/history/v1"
    price_map = {}
    page = 1
    has_more = True
    
    # We fetch all pools to ensure we get the subnets we need
    while has_more:
        params = {
            "timestamp_start": start_ts,
            "timestamp_end": end_ts,
            "limit": 256, # Max limit to reduce pages
            "page": page
        }
        
        try:
            resp = requests.get(url, headers=HEADERS, params=params)
            if resp.status_code != 200:
                print(f"⚠️ Price fetch failed: {resp.status_code}")
                break
            
            data = resp.json()
            items = data.get('data', [])
            
            if not items: break
            
            for item in items:
                netuid = item.get('netuid')
                price = float(item.get('price', 0))
                
                # Logic: We might get multiple price points for the same subnet in 24h.
                # We overwrite to keep the LATEST one (closest to end of day).
                # (Assuming the API returns chronological or we just take the last one seen)
                price_map[netuid] = price
            
            # Pagination Check
            if data.get('pagination', {}).get('next_page'):
                page += 1
                time.sleep(1.5)
            else:
                has_more = False
                
        except Exception as e:
            print(f"Error fetching prices: {e}")
            break
            
    return price_map

def fetch_miners_snapshot(subnet_id, start_ts, end_ts):
    """
    Fetches miner stats (emission, stake) with strict pagination.
    """
    url = f"{BASE_URL}/metagraph/history/v1"
    all_miners = []
    page = 1
    has_more = True
    miner_dict = {} 

    while has_more:
        params = {
            "netuid": subnet_id,
            "timestamp_start": start_ts,
            "timestamp_end": end_ts,
            "limit": 256,
            "page": page
        }
        
        try:
            resp = requests.get(url, headers=HEADERS, params=params)
            
            if resp.status_code == 429:
                time.sleep(1)
                continue
            if resp.status_code != 200:
                print(f"❌ Error {resp.status_code} fetching miners for SN{subnet_id}")
                break
            
            data = resp.json()
            items = data.get('data', [])
            
            if not items:
                break
            
            for item in items:
                uid = item.get('uid')
                # Deduplication: Keep the latest block entry for this UID
                if uid in miner_dict:
                    if item.get('block_number') > miner_dict[uid].get('block_number'):
                        miner_dict[uid] = item
                else:
                    miner_dict[uid] = item
            
            # --- PAGINATION LOGIC ---
            # This ensures we loop through Page 1, 2, 3... until next_page is Null
            if data.get('pagination', {}).get('next_page'):
                page += 1
                time.sleep(1.5)
            else:
                has_more = False
                
        except Exception as e:
            print(f"Connection error SN{subnet_id}: {e}")
            break
            
    return list(miner_dict.values())

def main():
    print("--- STARTING DTAO-AWARE DATA COLLECTION (V6) ---")
    
    # Date Setup
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_date_dt = datetime.strptime(END_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    
    all_records = []
    total_weeks = int((end_date_dt - current_date).days / 7)
    
    with tqdm(total=total_weeks, desc="Processing Weeks") as pbar:
        
        while current_date < end_date_dt:
            date_str = current_date.strftime("%Y-%m-%d")
            ts_start = int(current_date.timestamp())
            ts_end = ts_start + 86400 # 24h window
            
            # 1. Fetch ALL Prices for this day first (Efficient)
            # This saves us from making 60 separate calls inside the loop
            daily_prices = fetch_daily_prices(ts_start, ts_end)
            
            for netuid in SUBNETS:
                # Get price from our lookup map, default to 1.0 (Pre-dTao)
                # If dTao is active but price missing, it might mean the pool wasn't initialized yet
                alpha_price = daily_prices.get(netuid, 1.0)
                
                # 2. Fetch Miners
                miners = fetch_miners_snapshot(netuid, ts_start, ts_end)
                
                if miners:
                    for m in miners:
                        try:
                            emission_raw = float(m.get('emission', 0))
                            stake_raw = float(m.get('stake', 0))
                            # NEW: Grab incentive to prove "Intelligence" correlation
                            incentive = float(m.get('incentive', 0)) 
                            consensus = float(m.get('consensus', 0))
                            
                            # Clean trust immediately
                            trust = float(m.get('trust', 0) or 0.0)

                            daily_emission_tao = (emission_raw * 7200 / 1e9) * alpha_price
                            stake_tao_value = (stake_raw / 1e9) * alpha_price
                            
                            record = {
                                'date': date_str,
                                'block': m.get('block_number'),
                                'netuid': netuid,
                                'uid': m.get('uid'),
                                # 'coldkey': ... (Keep if analyzing entity concentration, crucial for Gini!)
                                'hotkey': m.get('hotkey', {}).get('ss58'),
                                'stake_tao_value': stake_tao_value,
                                'emission_daily_tao': daily_emission_tao,
                                'incentive': incentive,    # <--- Added
                                'consensus': consensus,    # <--- Added
                                'trust': trust,           # <--- Fixed type
                                'alpha_price': alpha_price, 
                                'active': m.get('active'),
                            }
                            all_records.append(record)
                        except Exception:
                            continue

                time.sleep(1.5) # Rate limit safety per subnet
            
            pbar.update(1)
            pbar.set_postfix_str(f"Date: {date_str}")
            current_date += timedelta(days=7)

    # Save
    if all_records:
        df = pd.DataFrame(all_records)
        df.to_parquet(OUTPUT_FILE, engine='fastparquet', compression='snappy')
        print(f"\n✅ Success! Saved {len(df)} records.")
        
        # Validation Print
        print("\n--- Data Sample ---")
        print(df[['date', 'netuid', 'alpha_price', 'emission_daily_tao']].tail())
    else:
        print("\n⚠️ No data collected.")

if __name__ == "__main__":
    main()