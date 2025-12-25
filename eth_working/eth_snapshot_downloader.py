import requests
import json
from pathlib import Path

NODE_URL = "http://10.105.50.169:5052"
OUTPUT_FILE = "eth_rich_snapshot.json"

def fetch_rich_snapshot():
    print(f"--- Fetching Full State from {NODE_URL} ---")
    # We query 'head' to get the latest state
    url = f"{NODE_URL}/eth/v1/beacon/states/head/validators"
    
    try:
        print("Downloading... (This handles ~200MB of data, please wait)")
        with requests.Session() as s:
            r = s.get(url, stream=True)
            r.raise_for_status()
            data = r.json()
            
        validators = data.get("data", [])
        print(f"✅ Success! Downloaded {len(validators)} validators.")
        
        # Save raw
        with open(OUTPUT_FILE, "w") as f:
            json.dump(data, f)
        print(f"Saved to {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    fetch_rich_snapshot()