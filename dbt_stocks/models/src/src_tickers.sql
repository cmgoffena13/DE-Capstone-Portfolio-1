WITH raw_stock_tickers AS (
    SELECT * FROM {{ source('stock', 'tickers') }}
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
    Row_Hash,
    watermark_timestamp
FROM raw_stock_tickers