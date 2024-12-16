WITH src_gov_official_trades AS (
    SELECT * FROM {{ ref('src_gov_official_trades') }}
)
SELECT
    trade_record_id,
    notification_date,
    ownership,
    report_date,
    report_id,
    security_name,
    security_ticker,
    security_type,
    amount AS amount_value_bucket,
    TO_NUMBER(REPLACE(REPLACE(REGEXP_SUBSTR(amount, '\\$[0-9,]+', 1, 1), '$', ''), ',', '')) AS minimum_value,
    TO_NUMBER(REPLACE(REPLACE(REGEXP_SUBSTR(amount, '\\$[0-9,]+', 1, 2), '$', ''), ',', '')) AS maximum_value,
    transaction_date,
    transaction_id,
    CASE 
        WHEN transaction_type = 'P' THEN 'Purchase'
        WHEN transaction_type = 'S' THEN 'Sale'
        WHEN transaction_type = 'S (Partial)' THEN 'Partial Sale'
        ELSE 'UNKNOWN'
    END AS transaction_type,
    TO_TIMESTAMP(record_updated) AS record_upated_utc,
    member_id
FROM src_gov_official_trades
WHERE security_ticker IS NOT NULL