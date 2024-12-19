{{
    config(
        tags=["src"]
    )
}}
WITH raw_gov_official_trades AS (
    SELECT * FROM {{ source('stock', 'gov_official_trades') }}
)
/*
Remove non-analytical columns, such as URLs and fix blanks space issues
*/
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
    leadership_positions,
    member_id,
    member_name,
    party,
    state,
    member_updated,
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
    transaction_type,
    record_updated
FROM raw_gov_official_trades