WITH src_gov_official_trades AS (
    SELECT * FROM {{ ref('src_gov_official_trades') }}
),
src_market_close_by_day as (
    SELECT * FROM {{ ref('src_market_close_by_day') }}
)
SELECT
    g.trade_record_id,
    g.notification_date,
    g.ownership,
    g.report_date,
    g.report_id,
    g.security_name,
    g.security_ticker,
    g.security_type,
    g.amount AS amount_value_bucket,
    TO_NUMBER(REPLACE(REPLACE(REGEXP_SUBSTR(g.amount, '\\$[0-9,]+', 1, 1), '$', ''), ',', '')) AS minimum_value,
    TO_NUMBER(REPLACE(REPLACE(REGEXP_SUBSTR(g.amount, '\\$[0-9,]+', 1, 2), '$', ''), ',', '')) AS maximum_value,
    ROUND((
        TO_NUMBER(REPLACE(REPLACE(REGEXP_SUBSTR(g.amount, '\\$[0-9,]+', 1, 2), '$', ''), ',', '')) + 
        TO_NUMBER(REPLACE(REPLACE(REGEXP_SUBSTR(g.amount, '\\$[0-9,]+', 1, 1), '$', ''), ',', ''))
    ) / 2) AS median_value,
    g.transaction_date,
    g.transaction_id,
    CASE 
        WHEN g.transaction_type = 'P' THEN 'Purchase'
        WHEN g.transaction_type = 'S' THEN 'Sale'
        WHEN g.transaction_type = 'S (Partial)' THEN 'Partial Sale'
        ELSE 'UNKNOWN'
    END AS transaction_type,
    CASE WHEN g.transaction_type = 'P' THEN 1 ELSE 0 END AS Is_Purchase,
    CASE WHEN g.transaction_type IN ('S', 'S (Partial)') THEN 1 ELSE 0 END AS Is_Sale,
    TO_TIMESTAMP_TZ(g.record_updated) AS record_upated_utc,
    g.member_id
FROM src_gov_official_trades AS g
WHERE g.security_ticker IS NOT NULL