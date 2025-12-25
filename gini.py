import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load your CLEAN, NORMALIZED data
# (Ensure you use the dataframe from the previous step)
df = pd.read_parquet("bittensor_sn1_post.parquet") 

# ---------------------------------------------------------
# PRE-PROCESSING (Repeated from your validation success)
# ---------------------------------------------------------
# 1. Filter for valid miners (Incentive > 0 is the ground truth)
df = df[df['incentive'] > 0].copy()

# 2. Normalize shares (We calculate 'share' of the day's total incentive)
daily_total = df.groupby('date')['incentive'].transform('sum')
df['share'] = df['incentive'] / daily_total

# ---------------------------------------------------------
# GINI CALCULATION FUNCTION
# ---------------------------------------------------------
def gini(array):
    """Calculate the Gini coefficient of a numpy array."""
    # based on bottom-up integration
    array = array.flatten()
    if np.amin(array) < 0:
        array -= np.amin(array) # Values cannot be negative
    array += 0.0000001 # Values cannot be 0
    array = np.sort(array)
    index = np.arange(1, array.shape[0] + 1)
    n = array.shape[0]
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array)))

# ---------------------------------------------------------
# APPLY PER DAY
# ---------------------------------------------------------
gini_results = []
dates = df['date'].unique()

print(f"Calculating Gini for {len(dates)} days...")

for d in sorted(dates):
    # Get all miner shares for this specific day
    day_shares = df[df['date'] == d]['share'].values
    
    # Calculate Gini
    g = gini(day_shares)
    
    # Count active miners (N) to see if concentration correlates with participation
    n_miners = len(day_shares)
    
    gini_results.append({'date': d, 'gini': g, 'miner_count': n_miners})

df_gini = pd.DataFrame(gini_results)

# ---------------------------------------------------------
# PLOT
# ---------------------------------------------------------
plt.figure(figsize=(12, 6))

# Plot Gini
plt.subplot(2, 1, 1)
plt.plot(pd.to_datetime(df_gini['date']), df_gini['gini'], color='purple', label='Gini Coefficient')
plt.title('Subnet 1: Concentration of Intelligence (Gini) Over Time')
plt.ylabel('Gini (0=Equal, 1=Centralized)')
plt.grid(True)
plt.legend()

# Plot Miner Count (To see if low N explains high Gini)
plt.subplot(2, 1, 2)
plt.bar(pd.to_datetime(df_gini['date']), df_gini['miner_count'], color='orange', alpha=0.6, label='Active Miners (Incentive > 0)')
plt.ylabel('Count')
plt.xlabel('Date')
plt.grid(True)
plt.legend()

plt.tight_layout()
plt.show()

# Print Averages
print(f"Average Gini: {df_gini['gini'].mean():.4f}")