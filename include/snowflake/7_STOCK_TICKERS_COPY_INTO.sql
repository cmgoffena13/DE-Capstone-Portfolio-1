COPY INTO STOCK_DB.staging.stock_tickers 
FROM (
    SELECT
        $1:active::BOOLEAN AS Active,
        $1:base_currency_name::STRING AS Base_Currency_Name,
        $1:base_currency_symbol::STRING AS Base_Currency_Symbol,
        $1:currency_name::STRING AS Currency_Name,
        $1:currency_symbol::STRING AS Currency_Symbol,
        $1:last_updated_utc::TIMESTAMP_TZ AS Last_Updated_UTC,
        $1:locale::STRING AS Locale,
        $1:market::STRING AS Market,
        $1:name::STRING AS Name,
        $1:ticker::STRING AS Ticker,
        SHA2_BINARY(
            CONCAT(
                IFNULL($1:active::STRING, ''),
                IFNULL($1:base_currency_name::STRING, ''),
                IFNULL($1:base_currency_symbol::STRING, ''),
                IFNULL($1:currency_name::STRING, ''),
                IFNULL($1:currency_symbol::STRING, ''),
                IFNULL($1:last_updated_utc::STRING, ''),
                IFNULL($1:locale::STRING, ''),
                IFNULL($1:market::STRING, ''),
                IFNULL($1:name::STRING, ''),
                IFNULL($1:ticker::STRING, '')
            )
        ) AS row_hash,
        SYSTIMESTAMP()
    FROM @STOCK_DB.external_stages.stock_tickers
    (FILE_FORMAT => 'STOCK_DB.file_formats.json_format')
)
PATTERN = '.*tickers.*'
ON_ERROR = 'SKIP_FILE_10%';