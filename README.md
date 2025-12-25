# ETH

- eth_snapshot_downloader.py downloads the current state of the eth networks validators
- The json contains all validators for each identity and it's corresponding registration date
- Using parse_eth_snapshot.py we can reconstruct a table with weekly entries for each entities validator count
- As rewards per validator remain roughly similar throughout the period, the sum of rewards can be estimated using the amount of validators
