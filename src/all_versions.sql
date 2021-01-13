BEGIN

CREATE TABLE all_version_final_deltas AS(
    select * from `v1`
    UNION ALL
    select * from `v2_dai`
    UNION ALL
    select * from `v2_sai`
    UNION ALL
    select * from `v2_usdc`
    UNION ALL 
    select * from `v3`
);

END;