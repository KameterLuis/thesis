import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# --- CONFIGURATION ---
INPUT_FILE = "eth_entity_history_part1.parquet"
OUTPUT_FILE = "eth_gini_daily.csv"

def gini(array):
    """Calculate the Gini coefficient of a numpy array."""
    array = array.flatten()
    if np.amin(array) < 0:
        array -= np.amin(array)
    array = array.astype(float) # Ensure float math
    array += 0.0000001
    array = np.sort(array)
    index = np.arange(1, array.shape[0] + 1)
    n = array.shape[0]
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array)))

def main():
    print(f"--- PROCESSING ETHEREUM GINI ({INPUT_FILE}) ---")
    
    # 1. Load Data
    df = pd.read_parquet(INPUT_FILE)
    
    # 2. Calculate Gini per Week
    gini_results = []
    
    # Group by the 'week' timestamp
    grouped = df.groupby('week')
    
    print(f"Calculating Gini for {len(grouped)} weeks...")
    
    for date_val, group in grouped:
        # The 'shares' are simply the validator counts per entity
        shares = group['validator_count'].values
        
        g = gini(shares)
        
        # We also track entity count (N) to show centralization vs participation
        entity_count = len(shares)
        total_validators = shares.sum()
        
        # Calculate 'Nakamoto Coefficient' proxy (Entity holding >33% or >51%)
        # Optional but powerful for thesis
        sorted_shares = np.sort(shares)[::-1] # Descending
        cumsum = np.cumsum(sorted_shares) / total_validators
        nakamoto_33 = np.searchsorted(cumsum, 0.33) + 1
        
        gini_results.append({
            'date': date_val,
            'gini': g,
            'entity_count': entity_count,
            'total_validators': total_validators,
            'nakamoto_33': nakamoto_33
        })
        
    df_gini = pd.DataFrame(gini_results)
    
    # 3. Upsample to Daily (Forward Fill)
    # This aligns the Weekly data to your Daily Bittensor timeline
    df_gini = df_gini.set_index('date').resample('D').ffill().reset_index()
    
    # 4. Save and Plot
    df_gini.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved daily Gini stats to {OUTPUT_FILE}")
    
    # Validation Print
    print("\n--- ETHEREUM STATS SAMPLE ---")
    print(df_gini[['date', 'gini', 'entity_count', 'nakamoto_33']].head())
    print(f"\nAverage Gini: {df_gini['gini'].mean():.4f}")
    
    # 5. Quick Visualization
    plt.figure(figsize=(10, 5))
    plt.plot(df_gini['date'], df_gini['gini'], label='Ethereum Gini (Stake)', color='blue')
    plt.title('Ethereum Staking Concentration (Entity Level)')
    plt.ylabel('Gini Coefficient')
    plt.ylim(0.0, 1.0) # Set limit to 0-1 for context
    plt.grid(True)
    plt.legend()
    plt.show()

if __name__ == "__main__":
    main()