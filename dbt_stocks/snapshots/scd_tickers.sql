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
    Active,
    Base_Currency_Name,
    Base_Currency_Symbol,
    Currency_Name,
    Currency_Symbol,
    Last_Updated_UTC,
    Locale,
    Market,
    Name,
    Ticker,
    --Row_Hash,
    watermark_timestamp
FROM src_tickers

{% endsnapshot %}