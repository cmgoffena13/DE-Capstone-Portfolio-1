{{
    config(
        tags=["int_audit"],
        materialized='table'
    )
}}
WITH src_market_close_by_day AS (
    SELECT * FROM {{ ref('src_market_close_by_day') }}
), src_gov_official_trades AS (
    SELECT * FROM {{ ref('src_gov_official_trades') }}
)
SELECT
    m.date_recorded,
    m.ticker,
    m.stock_open,
    m.stock_high,
    ROUND((m.stock_high + m.stock_low) / 2) AS stock_median,
    m.stock_low,
    m.stock_close,
    m.stock_volume
FROM src_market_close_by_day AS m
INNER JOIN src_gov_official_trades AS g
    ON g.security_ticker = m.ticker
    AND g.transaction_date = m.date_recorded
WHERE g.report_date = '{{ var("run_date") }}'