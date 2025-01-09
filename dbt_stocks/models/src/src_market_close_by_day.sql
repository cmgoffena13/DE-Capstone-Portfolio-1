{{
    config(
        tags=["src"]
    )
}}
WITH raw_market_close_by_day AS (
    SELECT * FROM {{ source('stock', 'market_close_by_day') }}
)

SELECT
    --status,
    date_recorded,
    symbol AS ticker,
    open AS stock_open,
    high AS stock_high,
    low AS stock_low,
    close AS stock_close,
    stock_volume,
    after_hours,
    pre_market
FROM raw_market_close_by_day
