BEGIN
CREATE TEMP FUNCTION
  PARSE_POD_LOG(data STRING,
    topics ARRAY<STRING>)
  RETURNS STRUCT<`timestamp` STRING,
  `collateral` STRING,
    `mantissa` STRING,
  `event` STRING>
  LANGUAGE js AS """
    switch (topics[ 0 ]) {    
    case '0x3c85af6bb3c5f67c404d53d1357995591840bcc7e5c21a96549351b80cd1b25e':
    var parsedEvent = { 'name': 'CollateralizationChanged',
      'inputs': [
        {'type': 'uint256', 'name': 'timestamp','indexed': true },
        {'type': 'uint256', 'name': 'tokens','indexed': false },
        {'type': 'uint256', 'name': 'collateral','indexed': false },
        {'type': 'uint256', 'name': 'mantissa','indexed': false }       
       ],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { timestamp: decoded.timestamp, collateral: decoded.collateral, mantissa: decoded.mantissa, event: 'CollateralizationChanged'};

  default:
    throw 'unexpected event decode';
}
""" OPTIONS ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

    -- get all Exchange rate event data this is the hardcoded data for the case statement below
    -- SELECT
    --     logs.block_timestamp AS block_timestamp,
    --     logs.block_number AS block_number,
    --     logs.transaction_hash AS transaction_hash,
    --     logs.log_index AS log_index,
    --     PARSE_POD_LOG(logs.DATA,
    --       logs.topics) AS parsed
    --   FROM
    --     `bigquery-public-data.crypto_ethereum.logs` AS logs
    --   WHERE
    --     address = '0x9f4c5d8d9be360df36e67f52ae55c1b137b4d0c4'
    --     AND topics[SAFE_OFFSET(0)] IN (
    --       '0x3c85af6bb3c5f67c404d53d1357995591840bcc7e5c21a96549351b80cd1b25e'

    --   )
    --   ORDER by parsed.timestamp;


        -- get token transfers FROM public dataset
      create temp table pod_transfers as(
        SELECT * 
        FROM(
            SELECT from_address as address,
            0 - CAST(value AS NUMERIC) as value, 
            transaction_hash, block_number, log_index 
            FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
            where token_address = "0x9f4c5d8d9be360df36e67f52ae55c1b137b4d0c4" 
            UNION ALL
            SELECT to_address as address,
            CAST(value AS NUMERIC) as value,
            transaction_hash, block_number, log_index 
            FROM `bigquery-public-data.crypto_ethereum.token_transfers`
            where token_address = "0x9f4c5d8d9be360df36e67f52ae55c1b137b4d0c4" 
        )
        WHERE address != "0x0000000000000000000000000000000000000000"
        AND block_number <  @v2_cutoff_block_number   
      );
      
      -- find corresponding exchange rate (calculated FROM collateral and mantissa)
     create temp table pod_normalised_transfers AS (
     SELECT 
      address,
      CAST(value AS numeric)/CAST(mantissa AS numeric) as value,
      transaction_hash,
      block_number,
      log_index
      FROM( 
        SELECT *,      
        CASE
            WHEN block_number < 10252642 THEN "997793854132195951469771" 
            WHEN block_number BETWEEN 10252642 AND 10478731 THEN "995588483418636972538758"
            WHEN block_number BETWEEN 10478732 AND 10750965 THEN "988223213554896415232677"
            WHEN block_number BETWEEN 10750966 AND 10887958 THEN "978829097919259472712343"
            WHEN block_number BETWEEN 10887959 AND 11068779 THEN "963562811684570071670400"
            WHEN block_number BETWEEN 11068780 AND 11114344 THEN "951954010827867951025026"
            WHEN block_number BETWEEN 11114344 AND 11205541 THEN "940551939199390322860274"
            WHEN block_number > 11205541 THEN "934144581645248069581929"
        END AS mantissa
      FROM
      `pod_transfers` 
      )
     );

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

CREATE TEMP TABLE pod_cutoff AS(
  SELECT address,
        0 as value,
        @v2_cutoff_block_number as block_number,
        0 as log_index,     
        0 as balance,
        prev_balance,
        delta_blocks
        FROM(
          SELECT address , 
          sum(value) as prev_balance,
          @v2_cutoff_block_number - max(block_number) as delta_blocks
          FROM  `v2_pod_delta_balances`
          GROUP BY address
  )
  WHERE prev_balance > 0
  ORDER BY address
);
CREATE TABLE v2_dai_pods AS (  
  SELECT 
    address,
    value * 1e16 as value, -- scaling to be equal to dai/sai
    block_number,
    log_index,
    balance,
    abs(prev_balance) * 1e16 as prev_balance, 
    delta_blocks
   FROM(
      SELECT * 
      FROM `v2_pod_delta_balances` 
      UNION ALL
      SELECT *
      FROM `pod_cutoff`
      ORDER BY address, block_number, log_index
    )
);

      
      
END;

