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
    SELECT "there are exactly 16,816 unique addresses" AS test_case,
        CAST((SELECT COUNT(distinct address) from all_earnings) AS STRING) as test_value,
           (CASE
                WHEN ((SELECT COUNT(distinct address) from all_earnings)) = 16816 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "no one gets less than the user_reward in POOL" AS test_case,
            CAST((SELECT MIN(earnings) FROM all_earnings) AS STRING) as test_value,
            (SELECT (MIN(earnings) = @user_reward) FROM all_earnings)
                                                    AS passes
    UNION ALL
    SELECT "there are no naughty list contracts in the rewards" AS test_case,
        CAST((SELECT COUNT(*) from all_earnings
              WHERE address NOT IN (SELECT DISTINCT address FROM naughty_list)) AS STRING) as test_value,
           (CASE
                WHEN (SELECT COUNT(*) from all_earnings
              WHERE address NOT IN (SELECT DISTINCT address FROM naughty_list)) = 0 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
                                                    );



                                                    SELECT *,
CASE 
WHEN earnings BETWEEN 20 AND 30 THEN "20-30"
WHEN earnings BETWEEN 30 AND 40 THEN "30-40"
WHEN earnings BETWEEN 40 AND 60 THEN "40-60" 
WHEN earnings BETWEEN 60 AND 80 THEN "60-80"
WHEN earnings BETWEEN 80 AND 100 THEN "80-100"
WHEN earnings BETWEEN 100 AND 200 THEN "100-200"
WHEN earnings BETWEEN 200 AND 300 THEN "200-300"
WHEN earnings BETWEEN 300 AND 400 THEN "300-400"     
WHEN earnings BETWEEN  400 AND 500 THEN "400-500" 
WHEN earnings BETWEEN 500 AND 600 THEN "500-600" 
WHEN earnings BETWEEN 600 AND 700 THEN "600-700" 
WHEN earnings BETWEEN 700 AND 800 THEN "700-800" 
WHEN earnings > 800 THEN ">800"
END
 FROM `psyched-ceiling-302219.retroactive_ea2485d17bc9f5c367c1176d29610c9fcc38e635.all_earnings` order by address
END;
