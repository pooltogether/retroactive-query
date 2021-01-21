BEGIN 

declare lp_total numeric;

declare total_tokens numeric;
declare token_floor numeric;
declare total_floor_tokens numeric;
declare remaining_lp_tokens numeric;

set total_tokens = 2e6;
set token_floor = 50;

create temp table scaled_down_deltas AS(
    SELECT 
        address,
        delta_blocks,
        prev_balance/1e10 as prev_balance,
        source 
        FROM `psyched-ceiling-302219.retroactive_9cbd3516e5f52c6d01e11c9ceffe6fc3e7b0c600.all_versions_final_deltas`
        where prev_balance > 0 AND delta_blocks > 0

);


-- finding total sum of overall lp and set variable
set lp_total = (SELECT sum(total_lp_shares)FROM(
    SELECT 
        address,
        sum(lp_shares) as total_lp_shares,
        source FROM(
            SELECT 
                address,
                prev_balance * delta_blocks as lp_shares,
                source
                FROM(
                    SELECT *  FROM `scaled_down_deltas`
                )
            )
        GROUP BY address, source
    ORDER BY address, source
)
);


-- get fraction of total lp over all versions/sources
CREATE TEMP TABLE lp_fractions AS(
    SELECT 
    address,
    IEEE_DIVIDE(sum(lp_shares),lp_total) as total_lp_shares_fraction,
    FROM(
        SELECT address,
        prev_balance * delta_blocks as lp_shares,
        source
        FROM(SELECT *  FROM `scaled_down_deltas`)
        )
        GROUP BY address
    ORDER BY total_lp_shares_fraction desc, address
);

# give token_floor amount to everyone that has participated in any non-zero way across all versions
set total_floor_tokens = (select sum(token_floor) from(
select address,
        token_floor as token_floor
        FROM (SELECT * FROM `lp_fractions`)
    )
);

set remaining_lp_tokens = total_tokens - total_floor_tokens;

# total_granted per address (floor + lp_fraction of remainder)
select *,
        total_granted/total_tokens as percentage_granted
        FROM(
            select *,
                    token_floor + lp_share as total_granted
                    FROM(
                        select address,
                            token_floor,
                            total_lp_shares_fraction * remaining_lp_tokens as lp_share
                            from (select * from `lp_fractions`)
                        )
            order by total_granted desc
); 


#now populate reason field -v1, v2, v3 etc.
create temp table reasons as(
    select address, STRING_AGG(source) as reasons 
    from(
        select address, source 
            from (select address, source, sum(prev_balance) from `scaled_down_deltas` group by address, source)
    )
    group by address
);

# combine rewards with reasons
create table all_earnings AS(
    select rewards.address,
            total_granted as earnings,
            reasons.reasons 
            FROM `rewards`
            INNER JOIN 
            `reasons`
            ON rewards.address = reasons.address
);

END;