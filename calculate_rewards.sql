

select address, sum(lp_shares) as total_lp_shares, source from(
select *, prev_balance * delta_blocks as lp_shares from(
    SELECT *  FROM `all_version_final_deltas`
)
group by address, source
order by address ASC;






