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

src_gov_official_trades AS (
    SELECT DISTINCT
        security_ticker,
        transaction_date
    FROM {{ ref('src_gov_official_trades') }}
    {% if is_incremental() %}
        WHERE report_date = '{{ var("run_date") }}'
    {% endif %}
),

audit AS (
    SELECT * FROM {{ ref('int_market_audit') }}
)

SELECT
    m.date_recorded,
    m.ticker,
    m.stock_open,
    m.stock_high,
    m.stock_low,
    m.stock_close,
    m.stock_volume,
    ROUND((m.stock_high + m.stock_low) / 2) AS stock_median
FROM src_market_close_by_day AS m
INNER JOIN src_gov_official_trades AS g
    ON
        m.ticker = g.security_ticker
        AND m.date_recorded = g.transaction_date
