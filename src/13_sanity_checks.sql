-- ensure no negative delta blocks or prev_balances
-- ensure no naughty list contracts in final DONE 
-- ensure exact number of participants DONE
-- ensure no one gets less than floor UPDATE FLOOR

BEGIN
    CREATE TABLE sanity_checks AS
    (
    SELECT "all earnings addresses are valid addresses" AS test_case,
    CAST((SELECT COUNT(1)
                      FROM all_earnings
                      WHERE NOT REGEXP_CONTAINS(address, "^0x[a-f0-9]{40}$")) AS STRING) AS test_value,
           (CASE
                WHEN (SELECT COUNT(1)
                      FROM all_earnings
                      WHERE NOT REGEXP_CONTAINS(address, "^0x[a-f0-9]{40}$")) > 0 THEN FALSE
                ELSE TRUE
               END)                                     AS passes
    UNION ALL
    SELECT "all earnings add up to exactly total reward" AS test_case,
        CAST((SELECT SUM(earnings)
                      FROM all_earnings) AS STRING) as test_value,
           (CASE
                WHEN (SELECT CAST(SUM(earnings) AS INT64)
                      FROM all_earnings) = 150000000 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "there are exactly 17,072 unique addresses" AS test_case,
        CAST((SELECT COUNT(distinct address) from all_earnings) AS STRING) as test_value,
           (CASE
                WHEN ((SELECT COUNT(distinct address) from all_earnings)) = 17072 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "no one gets less than the user_reward in POOL" AS test_case,
            CAST((SELECT MIN(earnings) FROM all_earnings) AS STRING) as test_value,
            (SELECT (MIN(earnings) = @token_floor) FROM all_earnings)
                                                    AS passes
    UNION ALL
    SELECT "there are no naughty list contracts in the rewards" AS test_case,
        CAST((SELECT COUNT(*) from all_earnings
              WHERE address NOT IN (SELECT DISTINCT address FROM naughty_list)) AS STRING) as test_value,
           (CASE
                WHEN (SELECT COUNT(*) from all_earnings
              WHERE address IN (SELECT DISTINCT address FROM naughty_list)) = 0 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
                                                    );


END;
