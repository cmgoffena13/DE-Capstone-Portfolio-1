WITH int_gov_officials AS (
    SELECT * FROM {{ ref('int_gov_officials') }}
),
int_gov_official_trades AS (
    SELECT * FROM {{ ref('int_gov_official_trades') }}
),
int_market AS (
    SELECT * FROM {{ ref('int_market') }}
)
SELECT
    g.member_id,
    g.chamber,
    g.member_name,
    g.party,
    g.state,
    g.district,
    g.committees,
    t.transaction_id,
    t.report_date,
    t.notification_date,
    t.transaction_date,
    t.transaction_type,
    t.security_ticker,
    t.security_name,
    t.minimum_value AS transaction_miminum_value,
    t.median_value AS transaction_median_value,
    t.maximum_value AS transaction_maximum_value,
    m.stock_open,
    m.stock_high,
    m.stock_median,
    m.stock_low,
    m.stock_close,
    m.stock_volume,
    t.Is_Purchase,
    t.Is_Sale
FROM int_gov_official_trades AS t
LEFT JOIN int_gov_officials AS g
    ON g.member_id = t.member_id
LEFT JOIN int_market AS m
    ON m.date_recorded = t.transaction_date
    AND m.ticker = t.security_ticker