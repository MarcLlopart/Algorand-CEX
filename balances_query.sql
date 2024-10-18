WITH daily_deltas AS (
    SELECT 
        toDate(a.realtime) AS td,
        b.entity_name,
        sum(a.microAlgosDelta::Int128) AS dd
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
        SUM(daily_deltas.dd / pow(10,6)) OVER (PARTITION BY entity_name ORDER BY td ) AS AlgoBalance
    FROM daily_deltas         
    ORDER BY td
    
) SELECT Date, 
      last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bitmax')) OVER(ORDER BY Date) AS `Bitmax`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Binance')) OVER(ORDER BY Date) AS `Binance`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'BingX')) OVER(ORDER BY Date) AS `BingX`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bitbox')) OVER(ORDER BY Date) AS `Bitbox`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bitfinex')) OVER(ORDER BY Date) AS `Bitfinex`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'BitGo')) OVER(ORDER BY Date) AS `BitGo`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bitkub')) OVER(ORDER BY Date) AS `Bitkub`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'BitPanda')) OVER(ORDER BY Date) AS `BitPanda`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bitrue')) OVER(ORDER BY Date) AS `Bitrue`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bitso')) OVER(ORDER BY Date) AS `Bitso`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bitstamp')) OVER(ORDER BY Date) AS `Bitstamp`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bittrex')) OVER(ORDER BY Date) AS `Bittrex`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bitvavo')) OVER(ORDER BY Date) AS `Bitvavo`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'BitZ')) OVER(ORDER BY Date) AS `BitZ`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'BKEX')) OVER(ORDER BY Date) AS `BKEX`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Blockchain.com')) OVER(ORDER BY Date) AS `Blockchain.com`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'BTC Markets')) OVER(ORDER BY Date) AS `BTC Markets`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'BTCTurk')) OVER(ORDER BY Date) AS `BTCTurk`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'ByBit')) OVER(ORDER BY Date) AS `ByBit`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Changelly')) OVER(ORDER BY Date) AS `Changelly`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Coinbase')) OVER(ORDER BY Date) AS `Coinbase`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Coinex')) OVER(ORDER BY Date) AS `Coinex`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Coinlist')) OVER(ORDER BY Date) AS `Coinlist`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'CoinSpot')) OVER(ORDER BY Date) AS `CoinSpot`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Crypto.com')) OVER(ORDER BY Date) AS `Crypto.com`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Gate.io')) OVER(ORDER BY Date) AS `Gate.io`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Hotbit')) OVER(ORDER BY Date) AS `Hotbit`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Indodax')) OVER(ORDER BY Date) AS `Indodax`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Koibanx')) OVER(ORDER BY Date) AS `Koibanx`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Korbit')) OVER(ORDER BY Date) AS `Korbit`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Kraken')) OVER(ORDER BY Date) AS `Kraken`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Kucoin')) OVER(ORDER BY Date) AS `Kucoin`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'MEXC')) OVER(ORDER BY Date) AS `MEXC`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Netcoins')) OVER(ORDER BY Date) AS `Netcoins`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'OkCoin')) OVER(ORDER BY Date) AS `OkCoin`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'OKX')) OVER(ORDER BY Date) AS `OKX`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Paribu')) OVER(ORDER BY Date) AS `Paribu`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Phemex')) OVER(ORDER BY Date) AS `Phemex`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'ProBit')) OVER(ORDER BY Date) AS `ProBit`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Ripio')) OVER(ORDER BY Date) AS `Ripio`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'UpHold')) OVER(ORDER BY Date) AS `UpHold`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Upbit')) OVER(ORDER BY Date) AS `Upbit`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Vindax')) OVER(ORDER BY Date) AS `Vindax`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'WhiteBIT')) OVER(ORDER BY Date) AS `WhiteBIT`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'AEX')) OVER(ORDER BY Date) AS `AEX`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Bilaxy')) OVER(ORDER BY Date) AS `Bilaxy`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Nobitex')) OVER(ORDER BY Date) AS `Nobitex`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Xeggex')) OVER(ORDER BY Date) AS `Xeggex`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'XT')) OVER(ORDER BY Date) AS `XT`,
 last_value(anyIf(toNullable(AlgoBalance), Entity = 'Blofin')) OVER(ORDER BY Date) AS `Blofin`
      ,sum(AlgoBalance) AS Total 
FROM aggregated_data 
GROUP BY Date 
ORDER BY Date
WITH FILL 
	STEP toIntervalDay(1)
	INTERPOLATE (`Total`)