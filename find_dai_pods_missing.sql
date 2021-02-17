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

create temp table pod_deposits as(
  select * from(
      SELECT
          logs.block_timestamp AS block_timestamp,
          logs.block_number AS block_number,
          logs.transaction_hash AS transaction_hash,
          logs.log_index AS log_index,
          PARSE_POD_LOG(logs.DATA,
            logs.topics) AS parsed
        FROM
          `bigquery-public-data.crypto_ethereum.logs` AS logs
        WHERE
          address = '0x9f4c5d8d9be360df36e67f52ae55c1b137b4d0c4'
          AND topics[SAFE_OFFSET(0)] IN (
            '0x116d2f47b438c3fc24b180b3b2136623f8821bfe4cb43f9a8c8068347868d084'
        )
  )
  where block_number < 11656283
);

create temp table pod_transfers as (
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
        AND block_number < 11656283
);


-- 270 results
select * from
(select parsed.from_address from `pod_deposits`
where parsed.from_address not in (select address from `pod_transfers`))
