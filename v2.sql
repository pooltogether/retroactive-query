   --Pool Dai @ 0x29fe7D60DdF151E5b52e5FAB4f1325da6b2bD958 topic0's
  -- Deposited: 0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4
  -- CommittedDepositWithdrawn: 0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70
  -- OpenDepositWithdawn: 0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004
  -- Withdawn: 0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5
CREATE TEMP FUNCTION
  PARSE_V2_LOG(data STRING,
    topics ARRAY<STRING>)
  RETURNS STRUCT<`address` STRING,
  `value` STRING,
  `event` STRING>
  LANGUAGE js AS """
switch (topics[ 0 ]) {
  //Deposited:  [{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}]
  case '0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4': 
    var parsedEvent = { 'name': 'Deposited',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true },
      {'type': 'uint256',
        'name': 'amount',
        'indexed': false}],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.amount, event: 'Deposited' };
  
  // Withdrawn: 0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5 {"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"Withdrawn","type":"event"}
  case '0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5':
    var parsedEvent = { 'name': 'Withdrawn',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true }, {
        'type': 'uint256',
        'name': 'amount',
        'indexed': false
      }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.amount, event: 'Withdrawn' };
    
  // OpenDepositWithdrawn 0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004 
  //{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"OpenDepositWithdrawn","type":"event"
  case '0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004':
    var parsedEvent = { 'name': 'OpenDepositWithdrawn',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true }, {
        'type': 'uint256',
        'name': 'amount',
        'indexed': false
      }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.amount, event: 'OpenDepositWithdrawn'};
    
    //CommittedDepositWithdrawn topic0: 0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70
    // {"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}
    case '0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70':
    var parsedEvent = { 'name': 'CommittedDepositWithdrawn',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true }, {
        'type': 'uint256',
        'name': 'amount',
        'indexed': false
      }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.amount, event: 'CommittedDepositWithdrawn'};
  default:
    throw 'unexpected event decode';
}
""" OPTIONS ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );
 
 
 
 -- all v2 Withdrawn events including v2.1 -2702 results -- saved as v2_all_withdrawn_events
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
        address = '0x29fe7d60ddf151e5b52e5fab4f1325da6b2bd958'
        AND topics[SAFE_OFFSET(0)] IN (
          '0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5'
      )


-- all v2.1 Withdrawn events -284 results -- saved as v2_1_withdrawn_events
SELECT * from( 
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
        address = '0x29fe7d60ddf151e5b52e5fab4f1325da6b2bd958'
        AND topics[SAFE_OFFSET(0)] IN (
          '0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5',
    '0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4',
      '0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004',
      '0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70'
      ))
         WHERE transaction_hash IN(
 SELECT  
    transaction_hash
  FROM (
    SELECT
      transaction_hash,
      COUNT(transaction_hash) AS num_tx
    FROM (
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
        address = '0x29fe7d60ddf151e5b52e5fab4f1325da6b2bd958'
    AND topics[SAFE_OFFSET(0)] IN (
    '0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5',
    '0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4',
      '0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004',
      '0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70') )
    GROUP BY
      transaction_hash )
  WHERE
    num_tx>1
 )
 AND parsed.event = "Withdrawn"


--v2.0 Withdrawn events -2418 results --saved as v2_0_withdrawn_events
select * from
`semiotic-cove-300720.retroactive_09195b91a75f4f29bcbc3aabb0811846a98db648.v2_all_withdrawn_events`  
WHERE transaction_hash NOT IN(select transaction_hash from 
`semiotic-cove-300720.retroactive_09195b91a75f4f29bcbc3aabb0811846a98db648.v2_1_withdrawn_events` )  


--v2 all other events excluding Withdrawn --21001 results --saved as v2_all_non_deposited_events
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
        address = '0x29fe7d60ddf151e5b52e5fab4f1325da6b2bd958'
        AND topics[SAFE_OFFSET(0)] IN (
    '0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4',
      '0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004',
      '0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70'
      )


--v2.0 deposits, v2.1 other events, 23419 results,, saved as v2_all_filtered_events
select * from 
(select * from `semiotic-cove-300720.retroactive_09195b91a75f4f29bcbc3aabb0811846a98db648.v2_all_non_deposited_events` 
UNION ALL
select * from `semiotic-cove-300720.retroactive_09195b91a75f4f29bcbc3aabb0811846a98db648.v2_0_withdrawn_events` 
)

-- sum v2 totals
SELECT
  parsed.event,
  SUM(CAST(parsed.value AS NUMERIC)/1e18) as totals
  FROM
  `semiotic-cove-300720.retroactive_09195b91a75f4f29bcbc3aabb0811846a98db648.v2_all_filtered_events`
  GROUP BY parsed.event
