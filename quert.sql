
WITH daily_deltas AS (
    SELECT 
        toDate(a.realtime) AS td,
        b.entity_name,
        sum(a.microAlgosDelta::Int128) AS dd,
        sumState(a.microAlgosDelta::Int128) AS dds
    FROM mainnet_agg.account_deltas_hourly a
    LEFT JOIN url('https://raw.githubusercontent.com/MarcLlopart/Algorand-CEX/refs/heads/main/data/walletbook_cex.csv') b 
      ON a.id = fn_addr2id_main(b.addr)
    WHERE b.is_cex = 1
    GROUP BY td, b.entity_name
    ORDER BY td, b.entity_name
),
-- Generate a list of dates for filling in missing days, converting UInt32 to Date
date_range AS (
    SELECT 
        toDate(arrayJoin(range(toUInt32(toDate(min(realtime))), toUInt32(toDate(now()))))) AS td
    FROM mainnet_agg.account_deltas_hourly
),
-- Cross join date range with entity names to ensure all combinations are present
entities AS (
    SELECT DISTINCT entity_name 
    FROM url('https://raw.githubusercontent.com/MarcLlopart/Algorand-CEX/refs/heads/main/data/walletbook_cex.csv')
    WHERE is_cex = 1
)
-- Final query with filled and interpolated results
SELECT 
    date_range.td Date, 
    entities.entity_name Entity, 
    runningAccumulate(daily_deltas.dds) / 1000000 AS AlgoBalance, 
    daily_deltas.dd / 1000000 AS AlgoBalanceDelta
FROM date_range
CROSS JOIN entities
LEFT JOIN daily_deltas 
    ON date_range.td = daily_deltas.td 
    AND entities.entity_name = daily_deltas.entity_name
where Entity='exchange_name'
ORDER BY date_range.td
with fill interpolate (AlgoBalance)