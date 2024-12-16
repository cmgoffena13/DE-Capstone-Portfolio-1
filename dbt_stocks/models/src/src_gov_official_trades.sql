/*
Remove non-analytical columns, such as URLs
*/
WITH raw_gov_official_trades AS (
    SELECT * FROM {{ source('stock', 'gov_official_trades') }}
)
SELECT
    --amendment_number,
    amount,
    chamber,
    --disclosure_url,
    committees,
    --display_name,
    district,
    --headshot,
    member_record_id,
    CASE WHEN leadership_positions = "[]" THEN NULL ELSE leadership_positions END AS leadership_positions,
    member_id,
    member_name,
    party,
    state,
    TO_TIMESTAMP(member_updated) AS member_updated_utc,
    --website,
    trade_record_id,
    notification_date,
    CASE WHEN ownership = '' THEN NULL ELSE ownership END AS ownership,
    report_date,
    report_id,
    security_name,
    CASE WHEN security_ticker = '' THEN NULL ELSE security_ticker END AS security_ticker,
    CASE WHEN security_type = '' THEN NULL ELSE security_type END AS security_type,
    transaction_date,
    transaction_id,
    CASE 
        WHEN transaction_type = 'P' THEN 'Purchase'
        WHEN transaction_type = 'S' THEN 'Sale'
        WHEN transaction_type = 'S (Partial)' THEN 'Partial Sale'
        ELSE 'Unknown'
    END AS transaction_type,
    TO_TIMESTAMP(record_updated) AS record_upated_utc
FROM raw_gov_official_trades