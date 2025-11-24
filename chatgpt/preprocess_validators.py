import os
import json
import pandas as pd
from pathlib import Path

VALIDATORS_JSON = Path("eth-validators.json")  # full snapshot file

TOP_ENTITY_COUNT = 20          # keep all validators from the top 20 entities
TAIL_SAMPLE_PER_ENTITY = 50    # sample at most 50 validators from each smaller entity
RANDOM_SEED = 42               # for reproducibility


def creds_to_entity(creds_hex: str, fallback_index: int) -> str:
    """
    Same logic as before:
    - If withdrawal_credentials starts with 0x01 or 0x02, last 20 bytes = ETH withdrawal address.
    - Else (legacy 0x00 credential) we can't extract the address, so treat that validator as its own entity.
    """
    raw = bytes.fromhex(creds_hex[2:])
    prefix = raw[0]
    if prefix in (1, 2) and len(raw) == 32:
        addr = "0x" + raw[12:].hex()
        return addr.lower()
    return f"legacy_{fallback_index}"


def load_validators_snapshot(validators_json_path: Path) -> pd.DataFrame:
    """
    Builds a dataframe:
      validator_index (int)
      entity_id (str)
    """
    with open(validators_json_path, "r") as f:
        raw = json.load(f)

    recs = []
    for v in raw["validators"]:
        idx = int(v["index"])
        entity_id = creds_to_entity(v["withdrawal_credentials"], idx)
        recs.append({
            "validator_index": idx,
            "entity_id": entity_id,
        })
    df = pd.DataFrame(recs)
    return df


def build_sampled_validator_set(df_all: pd.DataFrame,
                                top_entity_count: int = TOP_ENTITY_COUNT,
                                tail_sample_per_entity: int = TAIL_SAMPLE_PER_ENTITY,
                                rng_seed: int = RANDOM_SEED) -> pd.DataFrame:
    """
    Returns df_sample with columns:
      validator_index  (int)
      entity_id        (str)
      sample_weight    (float)

    How weighting works:
      - For whale entities (top N by validator count), we include *all* validators.
        Each validator gets weight = 1.0.
      - For tail entities, we sample up to 'tail_sample_per_entity' validators.
        Suppose entity X runs 5,000 validators, we sample 50.
        Then each sampled validator from X gets weight = 5000 / 50 = 100.
        That way, when we sum issuance/stake later and multiply by weight,
        we reconstruct that entity's total contribution.
    """

    # count validators per entity
    entity_counts = (
        df_all.groupby("entity_id")["validator_index"]
              .count()
              .rename("n_validators")
              .reset_index()
              .sort_values("n_validators", ascending=False)
              .reset_index(drop=True)
    )

    # identify whales
    whales = set(entity_counts.head(top_entity_count)["entity_id"].tolist())

    rng = pd.Series(range(len(df_all)))  # just to get a reproducible sample
    # (We'll use pandas' .sample(random_state=...) instead.)

    sampled_rows = []

    # process whales: keep them all, weight = 1
    whale_validators = df_all[df_all["entity_id"].isin(whales)].copy()
    whale_validators["sample_weight"] = 1.0
    sampled_rows.append(whale_validators)

    # process tail entities one by one
    tail_df = df_all[~df_all["entity_id"].isin(whales)].copy()
    for ent_id, group in tail_df.groupby("entity_id"):
        n_total = len(group)
        n_keep = min(n_total, tail_sample_per_entity)

        # sample n_keep validators from this entity
        group_sampled = group.sample(n=n_keep, random_state=rng_seed)

        # weight = (total validators this entity) / (sampled validators)
        weight = float(n_total) / float(n_keep)

        group_sampled = group_sampled.copy()
        group_sampled["sample_weight"] = weight

        sampled_rows.append(group_sampled)

    df_sample = pd.concat(sampled_rows, ignore_index=True)

    # (Optional sanity prints)
    print("Total validators overall:", len(df_all))
    print("Total entities overall:", entity_counts.shape[0])
    print("Whale entities kept in full:", len(whales))
    print("Sampled validator set size:", len(df_sample))
    print("Weighted sum of validators represented:",
          (df_sample["sample_weight"]).sum())

    # Save this so you can reuse it without re-sampling
    df_sample.to_parquet("eth_validator_sample.parquet", index=False)
    print("Saved eth_validator_sample.parquet")

    return df_sample


if __name__ == "__main__":
    df_all = load_validators_snapshot(VALIDATORS_JSON)
    df_sample = build_sampled_validator_set(df_all)
    print(df_sample)