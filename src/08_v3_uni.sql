
-- calcualted balance and prev_balance FROM public dataset 
BEGIN
CREATE TEMP TABLE v3_deltas AS (
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
        WHERE token_address = "0xa92a861fc11b99b24296af880011b47f9cafb5ab"
        AND block_number < @v3_cutoff_block_number 
        UNION ALL
        SELECT to_address as address,
        CAST(value AS NUMERIC) as value,
        transaction_hash, block_number, log_index 
        FROM `bigquery-public-data.crypto_ethereum.token_transfers`
        WHERE token_address = "0xa92a861fc11b99b24296af880011b47f9cafb5ab"
        AND block_number < @v3_cutoff_block_number 
    )

    WHERE address != "0x0000000000000000000000000000000000000000"
    ORDER BY address ASC
    )
);

--simulate total balance burn event for hard coded block if non-zero balance at this point
CREATE TEMP TABLE v3_simulated_balance_burn AS(
    SELECT address,
        0 as value,
        @v3_cutoff_block_number as block_number,
        0 as log_index,     
        0 as balance,
        prev_balance,
        delta_blocks
        FROM(
            SELECT address , 
            sum(value) as prev_balance,
            @v3_cutoff_block_number - max(block_number) as delta_blocks
            FROM  `v3_deltas`
            GROUP BY address
    )
    WHERE prev_balance > 0
    ORDER BY address
);

-- union the above two tables 
CREATE TABLE v3_uni AS(
    SELECT  * FROM(
    SELECT * 
    FROM `v3_simulated_balance_burn` 
    UNION ALL
    SELECT *
    FROM `v3_deltas`
    ORDER BY address, block_number, log_index
    )
);

END;