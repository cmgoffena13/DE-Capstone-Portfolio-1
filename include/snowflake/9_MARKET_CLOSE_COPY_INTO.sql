COPY INTO STOCK_DB.source.market_close_by_day 
FROM (
    SELECT
        $1:status::STRING AS status,
        $1:from::DATE AS date_recorded,
        $1:symbol::STRING AS symbol,
        $1:open::DECIMAL(20,4) AS open,
        $1:high::DECIMAL(20,4) AS high,
        $1:low::DECIMAL(20,4) AS low,
        $1:close::DECIMAL(20,4) AS close,
        $1:volume::BIGINT AS stock_volume,
        $1:afterhours::DECIMAL(20,4) AS after_hours,
        $1:premarket::DECIMAL(20,4) AS pre_market
    FROM @STOCK_DB.external_stages.market_close
    (FILE_FORMAT => 'STOCK_DB.file_formats.json_format')
)
PATTERN = '.*market_close.*'
ON_ERROR = 'SKIP_FILE_10%';