{{
    config(
        tags=["src"]
    )
}}
WITH raw_stock_tickers AS (
    SELECT * FROM {{ source('stock', 'tickers') }}
)

SELECT
    active,
    base_currency_name,
    base_currency_symbol,
    currency_name,
    currency_symbol,
    last_updated_utc,
    locale,
    market,
    name,
    ticker,
    --Row_Hash,
    watermark_timestamp
FROM raw_stock_tickers
