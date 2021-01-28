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

    //  SponsorshipDeposited 0x6dd4ea9218ce2f17ec77769fa65225b906e99dd3f597b7e087df3bdd8f7899dd
    case '0x6dd4ea9218ce2f17ec77769fa65225b906e99dd3f597b7e087df3bdd8f7899dd':
    var parsedEvent = { 'name': 'SponsorshipDeposited',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true }, {
        'type': 'uint256',
        'name': 'amount',
        'indexed': false
      }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.amount, event: 'SponsorshipDeposited'};

    //SponsorshipAndFeesWithdrawn 0x6a4d2bc0b4e5453e814890ffd34fde45f1820118a5e3e08c8273e6befd8cc050
    case '0x6a4d2bc0b4e5453e814890ffd34fde45f1820118a5e3e08c8273e6befd8cc050':
    var parsedEvent = { 'name': 'SponsorshipAndFeesWithdrawn',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true }, {
        'type': 'uint256',
        'name': 'amount',
        'indexed': false
      }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.amount, event: 'SponsorshipAndFeesWithdrawn'};

    //DepositedAndCommitted 0xc3a2b1de03156df25decfda8ed3e5aaa02ad33dc5fdf3f13aa9e7f6a7a8ae100
    case '0xc3a2b1de03156df25decfda8ed3e5aaa02ad33dc5fdf3f13aa9e7f6a7a8ae100':
    var parsedEvent = { 'name': 'DepositedAndCommitted',
      'inputs': [{ 'type': 'address', 'name': 'sender', 'indexed': true }, {
        'type': 'uint256',
        'name': 'amount',
        'indexed': false
      }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.sender, value: decoded.amount, event: 'DepositedAndCommitted'};

    
    case '0x39d270b67baa0bff7a394d3427e52a85d706cae15e649754ec7b54f3c9deb3f0':
    var parsedEvent = { 'name': 'Rewarded',
      'inputs': [
        { 'type': 'uint256', 'name': 'drawId', 'indexed': true },
        {'type': 'address', 'name': 'winner','indexed': true },
        {'type': 'bytes32', 'name': 'entropy','indexed': false },
        {'type': 'uint256', 'name': 'winnings','indexed': false },
        {'type': 'uint256', 'name': 'fee','indexed': false }
       
       ],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { address: decoded.winner, value: decoded.winnings, event: 'Rewarded'};

  default:
    throw 'unexpected event decode';
}
""" OPTIONS ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );
 
 
 
 -- all v2 Withdrawn events including v2.1

 CREATE TEMP TABLE v2_all_withdrawn_events AS(
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
        address = '0xb7896fce748396ecfc240f5a0d3cc92ca42d7d84'
        AND topics[SAFE_OFFSET(0)] IN (
          '0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5'
      )
 );

-- all v2.1 Withdrawn events
CREATE TEMP TABLE v2_1_withdrawn_events AS(
  SELECT * FROM( 
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
          address = '0xb7896fce748396ecfc240f5a0d3cc92ca42d7d84'
          AND topics[SAFE_OFFSET(0)] IN (
        '0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5',
        '0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4',
        '0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004',
        '0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70',
        '0x6dd4ea9218ce2f17ec77769fa65225b906e99dd3f597b7e087df3bdd8f7899dd',
        '0x6a4d2bc0b4e5453e814890ffd34fde45f1820118a5e3e08c8273e6befd8cc050',
        '0xc3a2b1de03156df25decfda8ed3e5aaa02ad33dc5fdf3f13aa9e7f6a7a8ae100'
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
          address = '0xb7896fce748396ecfc240f5a0d3cc92ca42d7d84'
      AND topics[SAFE_OFFSET(0)] IN (
        '0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5',
        '0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4',
        '0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004',
        '0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70',
        '0x6dd4ea9218ce2f17ec77769fa65225b906e99dd3f597b7e087df3bdd8f7899dd',
        '0x6a4d2bc0b4e5453e814890ffd34fde45f1820118a5e3e08c8273e6befd8cc050',
        '0xc3a2b1de03156df25decfda8ed3e5aaa02ad33dc5fdf3f13aa9e7f6a7a8ae100') )
      GROUP BY
        transaction_hash )
    WHERE
      num_tx>1
  )
  AND parsed.event = "Withdrawn"
);



--v2.0 Withdrawn events -2418 results --saved as v2_0_withdrawn_events
CREATE TEMP TABLE v2_0_withdrawn_events AS(
  SELECT * FROM
  `v2_all_withdrawn_events`  
  WHERE transaction_hash NOT IN(SELECT transaction_hash FROM 
  `v2_1_withdrawn_events` )
);  


--v2 all other events excluding Withdrawn
 CREATE TEMP TABLE v2_all_non_deposited_events AS (
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
        address = '0xb7896fce748396ecfc240f5a0d3cc92ca42d7d84'
        AND topics[SAFE_OFFSET(0)] IN (
      '0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4',
      '0x377533556d4ebd6be8b81e3573fd7e7bf70feb8737df314e8e7953cbb395f004',
      '0x5bd2fe46fdbb7534e8b97cffa63f641b75d3485cba0cfb856f0703409cf65e70',
      '0x6dd4ea9218ce2f17ec77769fa65225b906e99dd3f597b7e087df3bdd8f7899dd',
      '0x6a4d2bc0b4e5453e814890ffd34fde45f1820118a5e3e08c8273e6befd8cc050',
      '0xc3a2b1de03156df25decfda8ed3e5aaa02ad33dc5fdf3f13aa9e7f6a7a8ae100',
      '0x39d270b67baa0bff7a394d3427e52a85d706cae15e649754ec7b54f3c9deb3f0'
      )
 );

--v2.0 deposits, v2.1 other events
CREATE TEMP TABLE v2_all_filtered_events AS(
  SELECT * FROM 
    (SELECT * FROM `v2_all_non_deposited_events` 
      UNION ALL
    SELECT * FROM `v2_0_withdrawn_events` 
    )
);

-- sum v2 totals -- this query is for checking balance vs etherscan
-- SELECT
--   parsed.event,
--   SUM(CAST(parsed.value AS NUMERIC)/1e18) as totals
--   FROM
--   `v2_all_filtered_events`
--   GROUP BY parsed.event


-- now transforming events to transfers
--1. Deposited, DepositedAndCommited, Rewarded, SponsorshipDeposited are all "Mint" Transfers
--2. Use the Non-mint and non-burn Transfers FROM the token as-is
--3. Treat the Withdrawn, CommittedDepositWithdrawn, OpenDepositWithdrawn, SponsorshipandFeesWithdrawn all as "Burn" transfers
--So for 2) you'll want to pull in the transfers for the PoolToken where FROM != 0 and to != 0


-- get all "Transfer" events -- 35685 results
CREATE TEMP TABLE v2_all_synth_transfer_events AS(
  SELECT * FROM(
    SELECT
        from_address as address,
        0 - CAST(value AS NUMERIC) as value,
        transaction_hash, block_number, log_index,
        "Transfer" as event_type   
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    where token_address = "0xfe6892654cbb05eb73d28dcc1ff938f59666fe9f"
    and  to_address != "0x0000000000000000000000000000000000000000"
    and from_address != "0x0000000000000000000000000000000000000000"

    UNION ALL

    SELECT
        to_address as address,
        CAST(value AS NUMERIC) as value,
        transaction_hash, block_number, log_index,
        "Transfer" as event_type  
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    where token_address = "0xfe6892654cbb05eb73d28dcc1ff938f59666fe9f"
    and  to_address != "0x0000000000000000000000000000000000000000"
    and from_address != "0x0000000000000000000000000000000000000000"

    UNION ALL

    SELECT -- as per synth Burn 
          parsed.address as address,
          0 - CAST(parsed.value as NUMERIC) as value,
          transaction_hash,
          block_number,
          log_index,
          "Burn" as event_type
    FROM 
    (SELECT * FROM `v2_all_non_deposited_events` 
    UNION ALL
    SELECT * FROM `v2_0_withdrawn_events` 
    )
    where parsed.event = "Withdrawn" 
    OR parsed.event = "CommittedDepositWithdrawn" 
    OR parsed.event = "OpenDepositWithdrawn"
    OR parsed.event = "SponsorshipandFeesWithdrawn"

    UNION ALL

    SELECT -- as per synth Mint
          parsed.address as address,
          CAST(parsed.value as NUMERIC) as value,
          transaction_hash,
          block_number,
          log_index,
          "Mint" as event_type
    FROM 
    (SELECT * FROM `v2_all_non_deposited_events` 
    UNION ALL
    SELECT * FROM `v2_0_withdrawn_events` 
    )
    where parsed.event = "Deposited" 
    OR parsed.event = "DepositedAndCommitted" 
    OR parsed.event = "Rewarded"
    OR parsed.event = "SponsorshipDeposited"
  )
  WHERE value != 0 -- get rid of zero value transactions
  AND block_number < @v2_cutoff_block_number
  ORDER BY address, block_number, log_index ASC
);

-- now calculate rolling balances
CREATE TEMP TABLE v2_delta_balances AS(
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

    FROM`v2_all_synth_transfer_events`

    ORDER BY address ASC
  )
);

-- simulate cutoff event if these balances are still positive at the cutoff
CREATE TEMP TABLE v2_cutoff AS(
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
          FROM  `v2_delta_balances`
          GROUP BY address
  )
  WHERE prev_balance > 0
  ORDER BY address
);

-- union the cutoff burn event with the rest of the transfers
CREATE TABLE v2_sai AS(
  SELECT  * FROM(
    SELECT * 
    FROM `v2_delta_balances` 
    UNION ALL
    SELECT *
    FROM `v2_cutoff`
    ORDER BY address, block_number, log_index
  )
);

END;