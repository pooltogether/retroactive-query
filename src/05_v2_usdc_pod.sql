BEGIN 

CREATE TEMP FUNCTION
  PARSE_POD_LOG(data STRING,
    topics ARRAY<STRING>)
  RETURNS STRUCT<`from_address` STRING,
  `operator` STRING,
    `value` STRING,
  `event` STRING>
  LANGUAGE js AS """
    switch (topics[ 0 ]) {    
    case '0x116d2f47b438c3fc24b180b3b2136623f8821bfe4cb43f9a8c8068347868d084':
    var parsedEvent = { 'name': 'Deposited',
      'inputs': [
        {'type': 'address', 'name': 'operator','indexed': true },
        {'type': 'address', 'name': 'from','indexed': true },
        {'type': 'uint256', 'name': 'collateral','indexed': false },
        {'type': 'uint256', 'name': 'drawId','indexed': false },
        {'type': 'bytes', 'name': 'data','indexed': false },
        {'type': 'bytes', 'name': 'operatorData','indexed': false }        
       ],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { from_address: decoded.from, operator: decoded.operator, value: decoded.collateral, event: 'Deposited'};

  default:
    throw 'unexpected event decode';
}
""" OPTIONS ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

CREATE TEMP FUNCTION
  PARSE_POD_PENDING_DEPOSIT_WITHDRAWN_LOG(data STRING,
    topics ARRAY<STRING>)
  RETURNS STRUCT<`from_address` STRING,
  `operator` STRING,
    `value` STRING,
  `event` STRING>
  LANGUAGE js AS """
    switch (topics[ 0 ]) {    
    case '0x91f63202ac41673c1d492d91ee9bf7a27334ccbcf5bcfbeb5755c67a8d12a838':
    var parsedEvent = { 'name': 'Deposited',
      'inputs': [
        {'type': 'address', 'name': 'operator','indexed': true },
        {'type': 'address', 'name': 'from','indexed': true },
        {'type': 'uint256', 'name': 'collateral','indexed': false },
        {'type': 'bytes', 'name': 'data','indexed': false },
        {'type': 'bytes', 'name': 'operatorData','indexed': false }        
       ],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { from_address: decoded.from, operator: decoded.operator, value: decoded.collateral, event: 'Deposited'};

  default:
    throw 'unexpected event decode';
}
""" OPTIONS ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );



-- parse event logs for PendingDepositWithdrawn 1e6 values
create temp table pod_pending_withdrawals as(
  select * from(
      SELECT
          logs.block_timestamp AS block_timestamp,
          logs.block_number AS block_number,
          logs.transaction_hash AS transaction_hash,
          logs.log_index AS log_index,
          PARSE_POD_PENDING_DEPOSIT_WITHDRAWN_LOG(logs.DATA,
            logs.topics) AS parsed,
            "PendingDepositWithdrawn" as origin
        FROM
          `bigquery-public-data.crypto_ethereum.logs` AS logs
        WHERE
          address = '0x6f5587e191c8b222f634c78111f97c4851663ba4'
          AND topics[SAFE_OFFSET(0)] IN (
            '0x91f63202ac41673c1d492d91ee9bf7a27334ccbcf5bcfbeb5755c67a8d12a838'
        )
  )
  where block_number < 11656283
);

--parse events for Deposited event 1e6 scale values
create temp table pod_deposits as(
  select * from(
      SELECT
          logs.block_timestamp AS block_timestamp,
          logs.block_number AS block_number,
          logs.transaction_hash AS transaction_hash,
          logs.log_index AS log_index,
          PARSE_POD_LOG(logs.DATA,
            logs.topics) AS parsed,
            "Deposited" as origin
        FROM
          `bigquery-public-data.crypto_ethereum.logs` AS logs
        WHERE
          address = '0x6f5587e191c8b222f634c78111f97c4851663ba4'
          AND topics[SAFE_OFFSET(0)] IN (
            '0x116d2f47b438c3fc24b180b3b2136623f8821bfe4cb43f9a8c8068347868d084'
        )
  )
  where block_number < 11656283
);


-- get mint Transfers e12 values
create temp table pod_transfer_mints as(
    SELECT to_address as address,
            (CAST(value AS NUMERIC) * 1e12) as value,  -- scaling to 1e24
            transaction_hash, block_number, log_index,
            "Transfer-Mint" as origin 
            FROM `bigquery-public-data.crypto_ethereum.token_transfers`
            where token_address = "0x6f5587e191c8b222f634c78111f97c4851663ba4"
            and from_address = "0x0000000000000000000000000000000000000000"
            and to_address != "0x0000000000000000000000000000000000000000" -- filter out trasnfers to self
            and to_address != "0x6f5587e191c8b222f634c78111f97c4851663ba4"
            and block_number < 11656283
);

-- combine mint transfers and deposits together
create temp table all_pod_mints AS(
  select * from(
    select * from `pod_transfer_mints`
    UNION ALL 
        select 
          parsed.from_address as address,
          (CAST(parsed.value as NUMERIC)*1e18) as value, --1e18 to scale up for difference in base (USDC 1e6 vs POD event 1e24)
          transaction_hash,
          block_number,
          log_index,
          origin
        from `pod_deposits`
    )
  order by address, block_number, log_index
);


-- get transfers and burns from Public Transfer dataset 1e12 scale
create temp table pod_transfers as (
        SELECT * 
        FROM(
            #this part of the query gets non-mints transfers out from an address
            SELECT 
            from_address as address,
            (0 - CAST(value AS NUMERIC)*1e12) as value, 
            transaction_hash, block_number, log_index,
            CASE WHEN to_address = "0x0000000000000000000000000000000000000000" THEN "BurnTransferEvent"
            ELSE "p2p-Transfer-Out" END AS origin
            FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
            where token_address = "0x6f5587e191c8b222f634c78111f97c4851663ba4"
            and from_address != "0x0000000000000000000000000000000000000000"
            UNION ALL
            #this part of the query gets non-mint non-burn transfers in 
            SELECT to_address as address,
            (CAST(value AS NUMERIC)*1e12) as value, 
            transaction_hash, block_number, log_index,
            "p2p-Transfer-In" as origin 
            FROM `bigquery-public-data.crypto_ethereum.token_transfers`
            where token_address = "0x6f5587e191c8b222f634c78111f97c4851663ba4"
            and from_address != "0x0000000000000000000000000000000000000000"
            and to_address != "0x0000000000000000000000000000000000000000"
        )
        # WHERE address != "0x0000000000000000000000000000000000000000"
        WHERE block_number < 11656283
);


-- combine Withdrawn, pendingWithdrawn and transfer events together
create temp table all_pod_events as(
select    
  address,
  value, -- these have already been scaled to 1e24
  transaction_hash,
  block_number,
  log_index,
  origin
  from( 
    select 
      address,
      value, 
      transaction_hash,
      block_number,
      log_index,
      origin
    from `all_pod_mints`
    UNION ALL 
    select 
      parsed.from_address as address,
      (0 - CAST(parsed.value as NUMERIC) * 1e18) as value, -- 1e18 to account for difference in base (USDC Transfer 1e6 vs POD event 1e24)
      transaction_hash,
      block_number,
      log_index,
      origin
    from `pod_pending_withdrawals`
  )
  UNION ALL 
  select address,
  value,
  transaction_hash,
  block_number,
  log_index,
  origin
  from `pod_transfers`
  order by address, block_number, log_index
);


-- now filter out rows where transfer happened entirely within one block for the same value
create temp table all_events_non_zero_block_lengths as(
  select *  from `all_pod_events`
  where transaction_hash not in(
    select  transaction_hash from `all_pod_events`
    group by value, transaction_hash, block_number
    having count(*) > 1
  )
);


-- find corresponding exchange rate (calculated FROM collateral and mantissa)
-- this has the effect of /1e10 so as to prevent rounding to zero precision errors 
create temp table pod_normalised_transfers AS (
  SELECT 
  address,
  CAST(value AS numeric)/ CAST(mantissa AS numeric) as value, 
  transaction_hash,
  block_number,
  log_index
  FROM( 
    SELECT *,      
    CASE
        WHEN block_number < 10027387 THEN "9993210636.44775776713679" 
        WHEN block_number BETWEEN 10027387 AND 10040222 THEN "9989825252.45810425296122"
        WHEN block_number BETWEEN 10040222 AND 10065995 THEN "9987079469.32873941046141"
        WHEN block_number BETWEEN 10065995 AND 10117398 THEN "9982194529.43352911059711"
        WHEN block_number BETWEEN 10117398 AND 10169024 THEN "9974148447.14885566932754"
        WHEN block_number BETWEEN 10169024 AND 10362451 THEN "9973170713.40081993793812"
        WHEN block_number BETWEEN 10362451 AND 10375289 THEN "9970754597.72288395141677"
        WHEN block_number BETWEEN 10375289 AND 10414160 THEN "9956459821.87227020679637"
        WHEN block_number BETWEEN 10414160 AND 10465862 THEN "9923314114.09899697714436"
        WHEN block_number BETWEEN 10465862 AND 10907589 THEN "9901199420.45004674110412"
        WHEN block_number > 10907589 THEN "9894377931.94672047865689"
    END AS mantissa
  FROM
  `all_events_non_zero_block_lengths` 
  )
);


-- find rolling deltas on normalised dataset
  CREATE TEMP TABLE v2_pod_delta_balances AS(
  SELECT * , coalesce(LAG(balance,1) OVER
  (PARTITION BY address ORDER BY block_number, log_index),0) as prev_balance,
    coalesce(block_number - LAG(block_number,1) OVER
  (PARTITION BY address ORDER BY block_number, log_index),0) as delta_blocks 

  FROM(
    
    SELECT  address,
      value,
      block_number,
       log_index,
       SUM(value) OVER
           (PARTITION BY address ORDER BY block_number, log_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
           as balance

     FROM`pod_normalised_transfers`

    ORDER BY address ASC
  )
);

-- now insert cutoff block
CREATE TEMP TABLE pod_cutoff AS(
  SELECT address,
        0 as value,
        11656283 as block_number,
        0 as log_index,     
        0 as balance,
        prev_balance,
        delta_blocks
        FROM(
          SELECT address , 
          sum(value) as prev_balance,
          11656283 - max(block_number) as delta_blocks
          FROM  `v2_pod_delta_balances`
          GROUP BY address
  )
  WHERE prev_balance > 0
  ORDER BY address
);

-- union delta and cutoffs together and get those addresses with more than 2 entries
create temp table pod_deltas_and_cutoffs as(
     select *,
    -- "delta" as source
     from `v2_pod_delta_balances`
    UNION ALL
    select *,
    -- "cutoff" as source
    from `pod_cutoff`
);

-- filter out single entries (no cutoff defined because SUM(prev_balance) was zero)
-- 1e18 output
create table v2_usdc_pods AS(
    select 
      address,
      value,
      block_number,
      log_index,
      balance,
      (prev_balance * 1e4) as prev_balance, -- to scale back to 1e18
      delta_blocks  
      from `pod_deltas_and_cutoffs`
    where address in( 
        select address from `pod_deltas_and_cutoffs`
        group by address
        having count(address) > 1
    )
    order by address, block_number,log_index
);


-- scaling
-- Transfer events are 1e24, Deposit and PendingDepositWithdrawn are 1e6, Mantissa Normalisation 1e14

-- find unique addresses and see how many were missing from the original v2_dai_pod merkle claim set - should just get lp share
-- create table v2_usdc_pod_missing_only as(
--     select distinct address from `final_pod_deltas_and_cutoffs`
--     where address not in (select address from `future-system-305719.retroactive_e1edc1d67a341c9c865c89b162ece9fa63330ca4.v2_usdc_pods`)
--     order by address
-- );

-- -- find unique addresses and see how many were missing from the original all_earnings merkle claim set - should get floor plus lp share
-- create table v2_usdc_pod_received_no_reward as(
--     select distinct address from `final_pod_deltas_and_cutoffs`
--     where address not in (select address from `future-system-305719.retroactive_e1edc1d67a341c9c865c89b162ece9fa63330ca4.all_earnings_hexadecimal`)
--     order by address
-- );



END;

