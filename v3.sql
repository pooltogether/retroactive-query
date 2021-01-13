
-- calcualted balance and prev_balance from public dataset and saves as v3_deltas_1
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

  FROM(
    SELECT from_address as address,
     0 - CAST(value AS NUMERIC) as value, 
    transaction_hash, block_number, log_index 
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    where token_address = "0x334cbb5858417aee161b53ee0d5349ccf54514cf" 
    UNION ALL
    SELECT to_address as address,
    CAST(value AS NUMERIC) as value,
    transaction_hash, block_number, log_index 
    FROM `bigquery-public-data.crypto_ethereum.token_transfers`
    where token_address = "0x334cbb5858417aee161b53ee0d5349ccf54514cf" 
  )

  WHERE address != "0x0000000000000000000000000000000000000000"
  ORDER BY address ASC
)

--simulate total balance burn event for hard coded block if non-zero balance at this point, 3627 results
-- saved as v3_simulated_balance_burn
select address,
      0 as value,
      12000000 as block_number,
      0 as log_index,     
      0 as balance,
      prev_balance,
      delta_blocks
      from(
        select address , 
        sum(value) as prev_balance,
        12000000 - max(block_number) as delta_blocks
        from  `semiotic-cove-300720.retroactive_09195b91a75f4f29bcbc3aabb0811846a98db648.v3_deltas_1`
        GROUP BY address
)
WHERE prev_balance > 0
ORDER BY address

-- union the above two tables
select  * from(
  select * 
  from `semiotic-cove-300720.retroactive_09195b91a75f4f29bcbc3aabb0811846a98db648.v3_simulated_balance_burn` 
  UNION ALL
  select *
  from `semiotic-cove-300720.retroactive_09195b91a75f4f29bcbc3aabb0811846a98db648.v3_deltas_1`
  ORDER BY address, block_number, log_index
)

