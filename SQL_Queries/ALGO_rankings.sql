WITH daily_deltas AS (
    SELECT 
        toDate(a.realtime) AS td,
        b.entity_name,
        SUM(a.microAlgosDelta::Int128) AS dd
    FROM mainnet_agg.account_deltas_hourly a
    JOIN url('https://raw.githubusercontent.com/MarcLlopart/Algorand-CEX/refs/heads/main/data/walletbook_cex.csv') b 
      ON a.id = fn_addr2id_main(b.addr)
    WHERE b.is_cex = 1
    GROUP BY td, b.entity_name
    ORDER BY td, b.entity_name
),
aggregated_data AS (
    SELECT 
        td AS Date, 
        entity_name AS Entity, 
        SUM(daily_deltas.dd / pow(10,6)) OVER (PARTITION BY entity_name ORDER BY td) AS AlgoBalance
    FROM daily_deltas         
    ORDER BY td
),
CEX_balances AS (
    SELECT 
        Date,
        groupArray((Entity, AlgoBalance)) AS CEXArray,  -- Store exchanges dynamically in an array of tuples
        sum(AlgoBalance) AS Total -- Calculate total for each date
    FROM aggregated_data
    GROUP BY Date
    ORDER BY Date
    WITH FILL STEP toIntervalDay(1)
),
latest_data AS (
    SELECT 
        max(Date) AS LatestDate -- Get the most recent date
    FROM CEX_balances
),
previous_data AS (
    SELECT 
        Date,
        arrayConcat(CEXArray, [('Total', Total)]) AS CEXArray,  -- Add 'Total' to the CEXArray as a tuple
        Total
    FROM CEX_balances
    WHERE Date IN (
        (SELECT LatestDate FROM latest_data), -- Latest date
        (SELECT LatestDate FROM latest_data) - INTERVAL 1 DAY, -- 24 hours ago
        (SELECT LatestDate FROM latest_data) - INTERVAL 7 DAY, -- 7 days ago
        (SELECT LatestDate FROM latest_data) - INTERVAL 30 DAY, -- 30 days ago
        (SELECT LatestDate FROM latest_data) - INTERVAL 90 DAY  -- 90 days ago
    )
),
flattened_data AS (
    SELECT
        arrayJoin(CEXArray) AS CEXInfo,  -- Flatten the array into individual rows
        CEXInfo.1 AS Entity,             -- Extract the first element (Entity)
        CEXInfo.2 AS AlgoBalance,        -- Extract the second element (AlgoBalance)
        Date
    FROM previous_data
)
-- Step 1: CEX balances
SELECT 
    Entity,
    maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data)) AS LatestBalance, -- Latest balance for the CEX
    (maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data)) - 
     maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data) - INTERVAL 1 DAY)) /
    maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data) - INTERVAL 1 DAY) * 100 AS Change_24h,
    (maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data)) - 
     maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data) - INTERVAL 7 DAY)) /
    maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data) - INTERVAL 7 DAY) * 100 AS Change_7d,
    (maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data)) - 
     maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data) - INTERVAL 30 DAY)) /
    maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data) - INTERVAL 30 DAY) * 100 AS Change_30d,
    (maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data)) - 
     maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data) - INTERVAL 90 DAY)) /
    maxIf(AlgoBalance, Date = (SELECT LatestDate FROM latest_data) - INTERVAL 90 DAY) * 100 AS Change_90d
FROM flattened_data
GROUP BY Entity
ORDER BY LatestBalance DESC;