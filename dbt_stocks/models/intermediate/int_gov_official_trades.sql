{{
    config(
        tags=["int"],
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}
WITH src_gov_official_trades AS (
    SELECT * FROM {{ ref('src_gov_official_trades') }}
),

audit AS (
    SELECT * FROM {{ ref('int_gov_official_trades_audit') }}
)

SELECT
    g.trade_record_id,
    g.notification_date,
    g.ownership,
    g.report_date,
    g.security_name,
    g.security_ticker,
    g.security_type,
    g.amount AS amount_value_bucket,
    g.transaction_date,
    g.transaction_id,
    g.member_id,
    CAST(g.report_id AS STRING) AS report_id,
    TO_NUMBER(
        REPLACE(
            REPLACE(REGEXP_SUBSTR(g.amount, '\\$[0-9,]+', 1, 1), '$', ''),
            ',',
            ''
        )
    ) AS minimum_value,
    TO_NUMBER(
        REPLACE(
            REPLACE(REGEXP_SUBSTR(g.amount, '\\$[0-9,]+', 1, 2), '$', ''),
            ',',
            ''
        )
    ) AS maximum_value,
    ROUND((
        TO_NUMBER(
            REPLACE(
                REPLACE(REGEXP_SUBSTR(g.amount, '\\$[0-9,]+', 1, 2), '$', ''),
                ',',
                ''
            )
        )
        + TO_NUMBER(
            REPLACE(
                REPLACE(REGEXP_SUBSTR(g.amount, '\\$[0-9,]+', 1, 1), '$', ''),
                ',',
                ''
            )
        )
    ) / 2) AS median_value,
    CASE
        WHEN g.transaction_type = 'P' THEN 'Purchase'
        WHEN g.transaction_type = 'S' THEN 'Sale'
        WHEN g.transaction_type = 'S (Partial)' THEN 'Partial Sale'
        ELSE 'UNKNOWN'
    END AS transaction_type,
    CASE WHEN g.transaction_type = 'P' THEN 1 ELSE 0 END AS is_purchase,
    CASE WHEN g.transaction_type IN ('S', 'S (Partial)') THEN 1 ELSE 0 END
        AS is_sale,
    TO_TIMESTAMP_TZ(g.record_updated) AS record_upated_utc
FROM src_gov_official_trades AS g
WHERE
    g.security_ticker IS NOT NULL
    {% if is_incremental() %}
        AND g.report_date = '{{ var("run_date") }}'
    {% endif %}
