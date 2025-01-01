COPY INTO STOCK_DB.source.government_trades 
FROM (
    SELECT
        $1:amendment_number::INT AS amendment_number,
        $1:amount::STRING AS amount,
        $1:chamber::STRING AS chamber,
        $1:disclosure_url::STRING AS disclosure_url,
        
        $1:filer_info:committees::VARIANT AS committees,
        $1:filer_info:display_name::STRING AS display_name,
        $1:filer_info:district::STRING AS district,
        $1:filer_info:headshot::STRING AS headshot,
        $1:filer_info:id::STRING AS member_record_id,
        $1:filer_info:leadership_positions::VARIANT AS leadership_positions,
        $1:filer_info:member_id::STRING AS member_id,
        $1:filer_info:member_name::STRING AS member_name,
        $1:filer_info:party::STRING AS party,
        $1:filer_info:state::STRING AS state,
        $1:filer_info:updated::BIGINT AS member_updated,
        $1:filer_info:website::STRING AS website,

        $1:id::STRING AS trade_record_id,
        $1:notification_date::DATE AS notification_date,
        $1:ownership::STRING AS ownership,
        $1:report_date::DATE AS report_date,
        $1:report_id::STRING AS report_id,
        $1:security:name::STRING AS security_name,
        $1:security:ticker::STRING AS security_ticker,
        $1:security:type::STRING AS security_type,
        $1:transaction_date::DATE AS transaction_date,
        $1:transaction_id::STRING AS transaction_id,
        $1:transaction_type::STRING AS transaction_type,
        $1:updated::BIGINT AS record_updated
    FROM @STOCK_DB.external_stages.government_trades
    (FILE_FORMAT => 'STOCK_DB.file_formats.json_format')
)
PATTERN = '.*government_trades.*'
--ON_ERROR = 'SKIP_FILE_10%';