BEGIN

CREATE TEMP FUNCTION
  PARSE_V2_LOG(data STRING,
    topics ARRAY<STRING>)
  RETURNS STRUCT<`address` STRING,
  `value` STRING,
  `event` STRING>
  LANGUAGE js AS """
switch (topics[ 0 ]) {
  //Deposited:  [{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}]
  case '0xbae340cbbad65e770c316b0667f457021a64c3d9bd20aa1361bf0f9ca3ef459e': 
    var parsedEvent = { 'name': 'BoughtTickets',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true },
      {'type': 'uint256',
        'name': 'count',
        'indexed': false},
        {'type': 'uint256',
        'name': 'totalPrice',
        'indexed': false}],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.totalPrice,  event: 'BoughtTickets' };
    
    case '0x6feafc3039dc558ee6f547c450318c2e74b8844cacf0f75c0f33df7360ba7b6e': 
    var parsedEvent = { 'name': 'BoughtTickets',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true },
      {'type': 'uint256',
        'name': 'amount',
        'indexed': false}],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.amount,  event: 'Withdrawn' };


  
  default:
    throw 'unexpected event decode';
}
""" OPTIONS ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );


CREATE TEMP TABLE v1_p1_events AS(
-- get Bought ticket events for Pool 1 
  SELECT
        logs.block_timestamp AS block_timestamp,
        logs.block_number AS block_number,
        logs.transaction_hash AS transaction_hash,
        logs.log_index AS log_index,
        PARSE_V2_LOG(logs.DATA,
          logs.topics) AS parsed
      FROM
        `bigquery-public-data.crypto_ethereum.logs` AS logs
      WHERE
        address = '0x275ff8a50bc5841b09ec9a34c955898cd9eb43c6'
        AND topics[SAFE_OFFSET(0)] IN (
    '0xbae340cbbad65e770c316b0667f457021a64c3d9bd20aa1361bf0f9ca3ef459e'
    )
);




-- get Bought ticket events for Pool 2
CREATE TEMP TABLE v1_p2_events AS(
  SELECT
        logs.block_timestamp AS block_timestamp,
        logs.block_number AS block_number,
        logs.transaction_hash AS transaction_hash,
        logs.log_index AS log_index,
        PARSE_V2_LOG(logs.DATA,
          logs.topics) AS parsed
      FROM
        `bigquery-public-data.crypto_ethereum.logs` AS logs
      WHERE
        address = '0x4fc604536134dc64718800361ecbca0df6cbfe08'
        AND topics[SAFE_OFFSET(0)] IN (
    '0xbae340cbbad65e770c316b0667f457021a64c3d9bd20aa1361bf0f9ca3ef459e'
      )
);


-- get Bought ticket events for Pool 3 
CREATE TEMP TABLE v1_p3_events AS(
  SELECT
        logs.block_timestamp AS block_timestamp,
        logs.block_number AS block_number,
        logs.transaction_hash AS transaction_hash,
        logs.log_index AS log_index,
        PARSE_V2_LOG(logs.DATA,
          logs.topics) AS parsed
      FROM
        `bigquery-public-data.crypto_ethereum.logs` AS logs
      WHERE
        address = '0x9e6b460de0a61d3eb836021283cd15a89fe2fb91'
        AND topics[SAFE_OFFSET(0)] IN (
    '0xbae340cbbad65e770c316b0667f457021a64c3d9bd20aa1361bf0f9ca3ef459e'
      )
);



-- get Bought ticket events for Pool 4
CREATE TEMP TABLE v1_p4_events AS(
  SELECT
        logs.block_timestamp AS block_timestamp,
        logs.block_number AS block_number,
        logs.transaction_hash AS transaction_hash,
        logs.log_index AS log_index,
        PARSE_V2_LOG(logs.DATA,
          logs.topics) AS parsed
      FROM
        `bigquery-public-data.crypto_ethereum.logs` AS logs
      WHERE
        address = '0xe6e4154d85c52a325f4f51cec6d0fd40b974e6ae'
        AND topics[SAFE_OFFSET(0)] IN (
    '0xbae340cbbad65e770c316b0667f457021a64c3d9bd20aa1361bf0f9ca3ef459e'
      )
);





-- super impose burn event, where v1_pool1_events is table of pool1 subquery -- repeat this for each pool in v1
-- since the pool was fixed duration, for each deposited event  

CREATE TEMP TABLE v1_p1_deltas AS(
    SELECT * , coalesce(LAG(balance,1) OVER
    (PARTITION BY address ORDER BY block_number, log_index),0) as prev_balance,
    coalesce(block_number - LAG(block_number,1) OVER
    (PARTITION BY address ORDER BY block_number, log_index),0) as delta_blocks 

    FROM(

    SELECT  address, value, block_number, log_index,
    SUM(value) OVER
        (PARTITION BY address ORDER BY block_number, log_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        as balance from(
    SELECT 
    null as block_timestamp,
    block_number,
    "0x" as transaction_hash,
    0 as log_index,
    address,
    0 - prev_balance as value,
    "SimBurn" as event
    FROM
    (SELECT
        parsed.address AS address,
        sum(CAST(parsed.value AS NUMERIC)) AS prev_balance,
        max(block_number) + @v1_pool_period as block_number
    
    FROM
        `v1_p1_events` 
        group by parsed.address)
    UNION ALL
    SELECT
        block_timestamp,
        block_number AS block_number,
        transaction_hash,
        log_index,
        parsed.address AS address,
        CAST(parsed.value AS NUMERIC) AS value,
        parsed.event AS event
        FROM
        `v1_p1_events`
        )
    )
    ORDER BY address ASC 
);

CREATE TEMP TABLE v1_p2_deltas AS(
    SELECT * , coalesce(LAG(balance,1) OVER
    (PARTITION BY address ORDER BY block_number, log_index),0) as prev_balance,
    coalesce(block_number - LAG(block_number,1) OVER
    (PARTITION BY address ORDER BY block_number, log_index),0) as delta_blocks 

    FROM(

    SELECT  address, value, block_number, log_index,
    SUM(value) OVER
        (PARTITION BY address ORDER BY block_number, log_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        as balance from(
    SELECT 
    null as block_timestamp,
    block_number,
    "0x" as transaction_hash,
    0 as log_index,
    address,
    0 - prev_balance as value,
    "SimBurn" as event
    FROM
    (SELECT
        parsed.address AS address,
        sum(CAST(parsed.value AS NUMERIC)) AS prev_balance,
        max(block_number) + @v1_pool_period as block_number
    
    FROM
        `v1_p2_events` 
        group by parsed.address)
    UNION ALL
    SELECT
        block_timestamp,
        block_number AS block_number,
        transaction_hash,
        log_index,
        parsed.address AS address,
        CAST(parsed.value AS NUMERIC) AS value,
        parsed.event AS event
        FROM
        `v1_p2_events`
        )
    )
    ORDER BY address ASC 
);

CREATE TEMP TABLE v1_p3_deltas AS(
    SELECT * , coalesce(LAG(balance,1) OVER
    (PARTITION BY address ORDER BY block_number, log_index),0) as prev_balance,
    coalesce(block_number - LAG(block_number,1) OVER
    (PARTITION BY address ORDER BY block_number, log_index),0) as delta_blocks 

    FROM(

    SELECT  address, value, block_number, log_index,
    SUM(value) OVER
        (PARTITION BY address ORDER BY block_number, log_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        as balance from(
    SELECT 
    null as block_timestamp,
    block_number,
    "0x" as transaction_hash,
    0 as log_index,
    address,
    0 - prev_balance as value,
    "SimBurn" as event
    FROM
    (SELECT
        parsed.address AS address,
        sum(CAST(parsed.value AS NUMERIC)) AS prev_balance,
        max(block_number) + 36000 as block_number
    
    FROM
        `v1_p3_events` 
        group by parsed.address)
    UNION ALL
    SELECT
        block_timestamp,
        block_number AS block_number,
        transaction_hash,
        log_index,
        parsed.address AS address,
        CAST(parsed.value AS NUMERIC) AS value,
        parsed.event AS event
        FROM
        `v1_p3_events`
        )
    )
    ORDER BY address ASC 
);


CREATE TEMP TABLE v1_p4_deltas AS(
    SELECT * , coalesce(LAG(balance,1) OVER
    (PARTITION BY address ORDER BY block_number, log_index),0) as prev_balance,
    coalesce(block_number - LAG(block_number,1) OVER
    (PARTITION BY address ORDER BY block_number, log_index),0) as delta_blocks 

    FROM(

    SELECT  address, value, block_number, log_index,
    SUM(value) OVER
        (PARTITION BY address ORDER BY block_number, log_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        as balance from(
    SELECT 
    null as block_timestamp,
    block_number,
    "0x" as transaction_hash,
    0 as log_index,
    address,
    0 - prev_balance as value,
    "SimBurn" as event
    FROM
    (SELECT
        parsed.address AS address,
        sum(CAST(parsed.value AS NUMERIC)) AS prev_balance,
        max(block_number) + @v1_pool_period as block_number
    
    FROM
        `v1_p4_events` 
        group by parsed.address)
    UNION ALL
    SELECT
        block_timestamp,
        block_number AS block_number,
        transaction_hash,
        log_index,
        parsed.address AS address,
        CAST(parsed.value AS NUMERIC) AS value,
        parsed.event AS event
        FROM
        `v1_p4_events`
        )
    )
    ORDER BY address ASC 
);

CREATE TABLE v1 AS(
  SELECT * from(
      SELECT * from `v1_p1_deltas`
      UNION ALL
      SELECT * from `v1_p2_deltas`
      UNION ALL
      SELECT * from `v1_p3_deltas`
      UNION ALL
      SELECT * from `v1_p4_deltas`
  )
  ORDER BY address, block_number, log_index ASC
);


END;

