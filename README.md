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

v1: fixed period of 36,000 blocks
v2: block number 11,104,391
v3: block number 11,656,283

Total distribution is aimed at `` POOL.

### All users

400 UNI goes to:

- any account that directly `call`s a Uniswap pair or a Uniswap router contract
- any address that transfers any liquidity provider tokens or pair tokens to a Uniswap pair or a Uniswap router contract
- any address that holds liquidity provider tokens for a non-zero number of seconds

### Liquidity providers

- all liquidity is weighted by ETH value of liquidity / total ETH value
- fixed reward rate per second to all LPs pro-rata
- total rewards to liquidity providers is `150_000_000` - amount to users

### SOCKS users

1000 UNI goes to:

- every address that burns any SOCKS
- every address that holds at least 1 SOCKS token
 
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

The blob containing all the proofs of the retroactive distribution can be found at 

