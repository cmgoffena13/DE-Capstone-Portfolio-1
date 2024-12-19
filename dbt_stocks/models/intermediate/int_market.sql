{{
    config(
        tags=["int"],
        materialized='incremental',
        unique_key=['date_recorded', 'ticker'],
        incremental_strategy='merge'
    )
}}
WITH src_market_close_by_day AS (
    SELECT * FROM {{ ref('src_market_close_by_day') }}
),
audit AS (
    SELECT * FROM {{ ref('int_market_audit') }}
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
{% if is_incremental() %}
    WHERE date_recorded = '{{ var("run_date") }}'
{% endif %}