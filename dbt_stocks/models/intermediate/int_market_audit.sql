{{
    config(
        tags=["int_audit"],
        materialized='table'
    )
}}
WITH src_market_close_by_day AS (
    SELECT * FROM {{ ref('src_market_close_by_day') }}
)
SELECT
    date_recorded,
    ticker,
    stock_open,
    stock_high,
    ROUND((stock_high + stock_low) / 2) AS stock_median,
    stock_low,
    stock_close,
    stock_volume
FROM src_market_close_by_day
WHERE date_recorded = '{{ var("run_date") }}'