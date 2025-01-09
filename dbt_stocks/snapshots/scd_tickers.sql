{% snapshot scd_tickers %}
{{ config(
    target_schema='public',
    unique_key='ticker',
    strategy='timestamp',
    updated_at='Last_Updated_UTC',
    invalidate_hard_deletes=True
) }}
WITH src_tickers AS (
    SELECT * FROM {{ ref('src_tickers') }}
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
FROM src_tickers
{% endsnapshot %}