import pandas as pd
from tabulate import tabulate

df = pd.read_parquet("eth_entity_history_weekly.parquet")

print(df.tail(20))  # see first 20 rows
# print(df)
print(max(df["validator_count"]))
# print(df.sample(20))  # see 20 random rows (nice to sanity check)
# print(df.dtypes)  # see column types
# print(df["week_end"].unique()[:10])

# one_week = df[df["week_chain"] == 219]  # for example
# print(tabulate(one_week))
