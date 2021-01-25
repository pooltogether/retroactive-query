BEGIN 

declare lp_total numeric;
declare total_floor_tokens numeric;
declare remaining_lp_tokens numeric;
declare lp_share_total FLOAT64;
declare number_of_snapshot_voters numeric;

create temp table scaled_down_deltas AS(
    SELECT 
        address,
        delta_blocks,
        prev_balance/1e10 as prev_balance,
        source 
        FROM `all_versions_final_deltas`
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

set total_floor_tokens = (SELECT sum(token_floor) FROM(
SELECT address,
        @token_floor as token_floor
        FROM (SELECT * FROM `lp_fractions`)
    )
);

set remaining_lp_tokens = @total_reward - total_floor_tokens;

set lp_share_total = (select 
    SUM(lp_share) as share
    FROM(
        SELECT 
            address,
            LOG((1 + total_lp_shares_fraction * 10000),2) as lp_share
            FROM (SELECT * FROM `lp_fractions`)
    )
);


-- now calculate awards for snapshot voting

set number_of_snapshot_voters = (SELECT COUNT(address) FROM `voted_at_least_once`);

CREATE TEMP TABLE snapshot_rewards AS(
    SELECT address,
    @total_reward_snapshot/number_of_snapshot_voters as snapshot_reward,
    "snapshot" as source
    FROM `voted_at_least_once`
);

-- calc rewards using LP weighted formala
CREATE TEMP TABLE rewards AS(
    SELECT address,
        @token_floor + lp_and_snapshot as total_granted
        FROM(
            SELECT address,
                    SUM(reward) as lp_and_snapshot
                    FROM(
                        SELECT *
                            FROM(
                                SELECT address,
                                    IEEE_DIVIDE(LOG((1 + total_lp_shares_fraction * 10000),2) ,lp_share_total) * remaining_lp_tokens as  reward
                                    FROM (SELECT * FROM `lp_fractions`)

                                    UNION ALL
                                    SELECT 
                                        address,
                                        snapshot_reward as reward
                                        FROM `snapshot_rewards`
                                )
                    )
                    GROUP BY address
        )
); 

-- now populate reason field -v1, v2, v3, snapshot etc.
CREATE TEMP TABLE reasons as(
    SELECT address, STRING_AGG(source) as reasons 
    FROM(
        SELECT address, source 
            FROM (SELECT address, source, sum(prev_balance) FROM `scaled_down_deltas` group by address, source)
        UNION ALL
        SELECT address, source 
            FROM (SELECT address, source from `snapshot_rewards`)    
    )
    GROUP BY address
);

-- combine rewards with reasons
CREATE TABLE all_earnings AS(
    SELECT rewards.address,
            total_granted as earnings,
            reasons.reasons 
            FROM `rewards`
            INNER JOIN 
            `reasons`
            ON rewards.address = reasons.address
);

END;