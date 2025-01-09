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
    report_date,
    report_id,
    security_name,
    transaction_date,
    transaction_id,
    transaction_type,
    record_updated,
    CASE WHEN ownership = '' THEN NULL ELSE ownership END AS ownership,
    CASE
        WHEN security_ticker = '' THEN NULL
        WHEN LENGTH(security_ticker) > 0 THEN security_ticker
    END AS security_ticker,
    CASE
        WHEN security_type = '' THEN NULL
        WHEN LENGTH(security_type) > 0 THEN security_type
    END AS security_type
FROM raw_gov_official_trades
