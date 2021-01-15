BEGIN

CREATE TABLE all_version_final_deltas AS(
    select *,
    "V1" as source
     from `v1`
    UNION ALL
    select *,
        "V2_Dai" as source
         from `v2_dai`
    UNION ALL
    select *,
        "V2_Sai" as source
         from `v2_sai`
    UNION ALL
    select *,
        "V2_USDC" as source
         from `v2_usdc`
    UNION ALL 
    select *,
        "V3_Uni" as source
         from `v3_uni`
             UNION ALL 
    select *,
        "V3_Dai" as source
         from `v3_dai`
             UNION ALL 
    select *,
        "V3_usdc" as source
         from `v3_usdc`
);

END;