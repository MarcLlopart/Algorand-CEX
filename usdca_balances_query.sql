 

WITH daily_deltas AS (
    SELECT 
        toDate(a.realtime) AS td,
        b.entity_name,
        sum(a.amtDelta::Int128) AS dd
    FROM mainnet_agg.account_asset_deltas_hourly a
    JOIN url('https://raw.githubusercontent.com/MarcLlopart/Algorand-CEX/refs/heads/main/data/walletbook_cex.csv') b 
      ON a.id = fn_addr2id_main(b.addr)
    WHERE b.is_cex = 1
          and a.asset = 31566704 --usdca id
    GROUP BY td, b.entity_name
    ORDER BY td, b.entity_name
),
aggregated_data AS (
    SELECT 
        td AS Date, 
        entity_name AS Entity, 
        SUM(daily_deltas.dd / pow(10,6)) OVER (PARTITION BY entity_name ORDER BY td ) AS USDCaBalance
    FROM daily_deltas         
    ORDER BY td
    
) SELECT Date, 
      last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bitmax')) OVER(ORDER BY Date) AS `Bitmax`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Binance')) OVER(ORDER BY Date) AS `Binance`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'BingX')) OVER(ORDER BY Date) AS `BingX`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bitbox')) OVER(ORDER BY Date) AS `Bitbox`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bitfinex')) OVER(ORDER BY Date) AS `Bitfinex`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'BitGo')) OVER(ORDER BY Date) AS `BitGo`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bitkub')) OVER(ORDER BY Date) AS `Bitkub`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'BitPanda')) OVER(ORDER BY Date) AS `BitPanda`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bitrue')) OVER(ORDER BY Date) AS `Bitrue`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bitso')) OVER(ORDER BY Date) AS `Bitso`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bitstamp')) OVER(ORDER BY Date) AS `Bitstamp`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bittrex')) OVER(ORDER BY Date) AS `Bittrex`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bitvavo')) OVER(ORDER BY Date) AS `Bitvavo`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'BitZ')) OVER(ORDER BY Date) AS `BitZ`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'BKEX')) OVER(ORDER BY Date) AS `BKEX`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Blockchain.com')) OVER(ORDER BY Date) AS `Blockchain.com`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'BTC Markets')) OVER(ORDER BY Date) AS `BTC Markets`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'BTCTurk')) OVER(ORDER BY Date) AS `BTCTurk`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'ByBit')) OVER(ORDER BY Date) AS `ByBit`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Changelly')) OVER(ORDER BY Date) AS `Changelly`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Coinbase')) OVER(ORDER BY Date) AS `Coinbase`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Coinex')) OVER(ORDER BY Date) AS `Coinex`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Coinlist')) OVER(ORDER BY Date) AS `Coinlist`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'CoinSpot')) OVER(ORDER BY Date) AS `CoinSpot`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Crypto.com')) OVER(ORDER BY Date) AS `Crypto.com`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Gate.io')) OVER(ORDER BY Date) AS `Gate.io`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Hotbit')) OVER(ORDER BY Date) AS `Hotbit`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Indodax')) OVER(ORDER BY Date) AS `Indodax`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Koibanx')) OVER(ORDER BY Date) AS `Koibanx`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Korbit')) OVER(ORDER BY Date) AS `Korbit`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Kraken')) OVER(ORDER BY Date) AS `Kraken`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Kucoin')) OVER(ORDER BY Date) AS `Kucoin`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'MEXC')) OVER(ORDER BY Date) AS `MEXC`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Netcoins')) OVER(ORDER BY Date) AS `Netcoins`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'OkCoin')) OVER(ORDER BY Date) AS `OkCoin`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'OKX')) OVER(ORDER BY Date) AS `OKX`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Paribu')) OVER(ORDER BY Date) AS `Paribu`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Phemex')) OVER(ORDER BY Date) AS `Phemex`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'ProBit')) OVER(ORDER BY Date) AS `ProBit`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Ripio')) OVER(ORDER BY Date) AS `Ripio`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'UpHold')) OVER(ORDER BY Date) AS `UpHold`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Upbit')) OVER(ORDER BY Date) AS `Upbit`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Vindax')) OVER(ORDER BY Date) AS `Vindax`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'WhiteBIT')) OVER(ORDER BY Date) AS `WhiteBIT`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'AEX')) OVER(ORDER BY Date) AS `AEX`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Bilaxy')) OVER(ORDER BY Date) AS `Bilaxy`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Nobitex')) OVER(ORDER BY Date) AS `Nobitex`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Xeggex')) OVER(ORDER BY Date) AS `Xeggex`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'XT')) OVER(ORDER BY Date) AS `XT`,
 last_value(anyIf(toNullable(USDCaBalance), Entity = 'Blofin')) OVER(ORDER BY Date) AS `Blofin`
      ,sum(USDCaBalance) AS Total 
FROM aggregated_data 
GROUP BY Date 
ORDER BY Date
WITH FILL 
	STEP toIntervalDay(1)
	INTERPOLATE (`Total`)
