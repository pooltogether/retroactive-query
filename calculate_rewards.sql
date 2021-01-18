# SELECT  * FROM `diesel-rhythm-302118.retroactive_620a6d294122fe7c81d3a1916b7cd7beec6c72f9.all_versions_final_deltas` where  prev_balance < 0
# order by address, block_number, log_index
BEGIN 
declare lp_total numeric; 
-- finding total sum of lp and set variable
set lp_total = (select sum(total_lp_shares)from(
    select address, sum(lp_shares/1e10) as total_lp_shares, source from(
        select address,
        prev_balance * delta_blocks as lp_shares,
        source
        from(
            SELECT *  FROM `diesel-rhythm-302118.retroactive_620a6d294122fe7c81d3a1916b7cd7beec6c72f9.all_versions_final_deltas`
        )
        )
        group by address, source
    order by address, source
)
);



select address, sum(lp_shares/1e10)/lp_total as total_lp_shares_fraction, source from(
        select address,
        prev_balance * delta_blocks as lp_shares,
        source
        from(
            SELECT *  FROM `diesel-rhythm-302118.retroactive_620a6d294122fe7c81d3a1916b7cd7beec6c72f9.all_versions_final_deltas`
        )
        )
        group by address, source
    order by address, source;
END;
