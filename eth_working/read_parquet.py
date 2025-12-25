import pandas as pd
from tabulate import tabulate

df = pd.read_parquet("eth_working/eth_entity_history_part1.parquet")
#df = pd.read_parquet("bittensor_sn1_pre.parquet")

print(df.tail(20))  # see first 20 rows
#print(df)
#print(max(df["consensus"]))
#print(max(df["incentive"]))
#print(max(df["validator_count"]))
# print(df.sample(20))  # see 20 random rows (nice to sanity check)
print(df.dtypes)  # see column types
# print(df["week_end"].unique()[:10])

# one_week = df[df["week_chain"] == 219]  # for example
# print(tabulate(one_week))
