BEGIN 


declare lp_total numeric; 
-- finding total sum of overall lp and set variable
set lp_total = (SELECT sum(total_lp_shares)FROM(
    SELECT 
        address,
        sum(lp_shares/1e10) as total_lp_shares,
        source FROM(
            SELECT 
                address,
                prev_balance * delta_blocks as lp_shares,
                source
                FROM(
                    SELECT *  FROM `all_versions_final_deltas`
                )
            )
        GROUP BY address, source
    ORDER BY address, source
)
);


-- get fraction of total lp
CREATE TABLE lp_fraction_of_total AS(
    SELECT 
    address,
    sum(lp_shares/1e10)/lp_total as total_lp_shares_fraction,
    source
    FROM(
        SELECT address,
        prev_balance * delta_blocks as lp_shares,
        source
        FROM(
            SELECT *  FROM `all_versions_final_deltas`
        )
        )
        GROUP BY address, source
    ORDER BY address, source
);






END;
