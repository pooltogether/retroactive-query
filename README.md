# @pooltogether/retroactive-query

[![Run Queries](https://github.com/pooltogether/retroactive-query/workflows/Run%20Queries/badge.svg)](https://github.com/pooltogether/retroactive-query/actions?query=workflow%3A%22Run+Queries%22)

This repository contains queries that produce the tables of retroactive PT token distributions.

The queries run in [Google BigQuery](https://cloud.google.com/bigquery) against the 
[`bigquery-public-data.crypto_ethereum`](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=crypto_ethereum&page=dataset) 
dataset.

Data for this dataset is extracted to Google BigQuery using
[blockchain-etl/ethereum-etl](https://github.com/blockchain-etl/ethereum-etl).

## Specifications

There are 3 versions of the PoolTogether protocol. The cutoff blocks are as follows:

v1: fixed period of 43,200 blocks (1 week - v1 pools were ephemeral)
v2 and v3: block number 11,656,283

Total distribution is aimed at `1,500,000` POOL.

### All users

20 UNI goes to:

- any account that directly withdrew or deposited into any version of the protocol, including the v2 pods

### Liquidity providers

- all liquidity is weighted by the time (measured in blocks) multiplied by the amount expressed as a percentage of total liquidity to give a `total_lp_shares_fraction` per address
- The additional rewarded tokens is then calculated as:
 `LOG2(1 + total_lp_shares_fraction * 10000)/lp_share_total) * remaining_lp_tokens` where `remaining_lp_tokens` is the number of tokens after the base reward of 20 POOL for each address has been subtracted from the total distribution.  
- This log influenced distribution allows for a distribution that is reduced for large providers and enhanced for smaller holders


## Reproduction

You can reproduce the results of this query by forking this repository and adding your own secrets to run in your own GCP account.

1. Create a Google Cloud project [here](https://cloud.google.com/) 
1. Find your Project ID in the Google Cloud console [here](https://console.cloud.google.com/)
1. Fork this repository
1. Add the secret `GCP_PROJECT_ID` under Settings > Secrets containing your project ID from the GCP dashboard 
1. Add the secret `GCP_SA_KEY` under Settings > Secrets containing the base64 encoded JSON key of a service account. This can be obtained by following [this] (https://cloud.google.com/iam/docs/reference/rest/v1/projects.serviceAccounts.keys) 
1. Go to the actions tab of your fork
1. Run the workflow (roughly ~10 minutes to complete)
1. Inspect the resulting tables - all_earnings_hexadecimal is the final table used in the merkle distribution. 
1. These results can be downloaded in JSON format and verified as to the merkle distribution [contract] (https://github.com/pooltogether/merkle-distributor)

### Determinism notes

Note that, for floating point input types, the return result of aggregations is non-deterministic,
which means you will not get the exact same result each time you aggregate floating point columns.

These queries make use of floating point numbers. However in the final `all_earnings_hexadecimal` table,
we truncate to 6 decimal places so that the result used for production is the same across multiple runs.

See
[https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions)
for more information.

### Final results

The blob containing all the proofs of the retroactive distribution can be found at [https://https://objective-jang-89749c.netlify.app/.netlify/functions/all] (https://https://objective-jang-89749c.netlify.app/.netlify/functions/all)

