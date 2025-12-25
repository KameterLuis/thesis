import pandas as pd
import matplotlib.pyplot as plt

INPUT_FILE = "bittensor_sn1_pre.parquet"

def validate_and_normalize():
    print(f"--- LOADING {INPUT_FILE} ---")
    df = pd.read_parquet(INPUT_FILE)
    
    # 1. SAFETY FILTER: Miners Only + Actually Working
    # We use 'incentive' > 0 to ensure they were actually producing value.
    # We use 'trust' < 0.05 to ensure they are Miners, not Validators.
    df_clean = df[
        (df['incentive'] > 0)# & 
        #(df['trust'] < 0.05)
    ].copy()
    
    # 2. NORMALIZATION (The Thesis Fix)
    # Since we know the API emission values are skewed/wrong units,
    # we reconstruct the emission based on the miner's SHARE of the total incentive.
    
    # A. Calculate Total Daily Incentive per Date
    daily_stats = df_clean.groupby('date')['incentive'].sum().reset_index()
    daily_stats.rename(columns={'incentive': 'total_daily_incentive'}, inplace=True)
    
    # B. Merge back to main DF
    df_clean = df_clean.merge(daily_stats, on='date', how='left')
    
    # C. Calculate "Share" of the network (0.0 to 1.0)
    df_clean['network_share'] = df_clean['incentive'] / df_clean['total_daily_incentive']
    
    # D. Force-Fit to Reality (Optional but good for visualization)
    # We assume Subnet 1 gets ~18% of the 7200 TAO network (Approx 1296 TAO)
    # (Note: For your Gini calc, you can just use 'network_share', the result is identical)
    ESTIMATED_SN1_EMISSION = 1296.0 
    df_clean['normalized_emission'] = df_clean['network_share'] * ESTIMATED_SN1_EMISSION
    
    print(df_clean['normalized_emission'])

    # 3. VERIFY
    daily_sums_clean = df_clean.groupby('date')['normalized_emission'].sum()
    
    print("\n--- NORMALIZED RESULTS ---")
    print(f"Original Raw Sum (Avg): {df.groupby('date')['emission_daily_tao'].sum().mean():.2f}")
    print(f"Normalized Sum (Avg):   {daily_sums_clean.mean():.2f}")
    
    # 4. PLOT
    plt.figure(figsize=(10, 5))
    plt.plot(daily_sums_clean.index, daily_sums_clean.values, label='Normalized Emission (Fixed)')
    plt.axhline(y=7200, color='r', linestyle='--', label='Network Max (7200)')
    plt.title('Subnet 1 Emission (Normalized by Incentive Share)')
    plt.ylabel('TAO')
    plt.legend()
    plt.grid(True)
    plt.show()
    
    return df_clean

if __name__ == "__main__":
    df_final = validate_and_normalize()
    # Save this df_final for your actual Gini analysis!
    # df_final.to_parquet("bittensor_sn1_normalized.parquet")