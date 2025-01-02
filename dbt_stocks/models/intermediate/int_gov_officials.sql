{{
    config(
        tags=["int"],
        materialized='incremental',
        unique_key='member_id',
        incremental_strategy='merge'
    )
}}
WITH gov_officials AS (
    SELECT * FROM {{ ref('src_gov_official_trades') }}
),
states_CTE AS (
    SELECT * FROM {{ ref('states') }}
),
audit AS (
    SELECT * FROM {{ ref('int_gov_officials_audit') }}
), deduped AS (
    SELECT
        g.member_id,
        CASE 
            WHEN g.chamber = 'House' THEN 'House of Representatives'
            WHEN g.chamber = 'Senate' THEN 'Senate'
            ELSE 'UNKNOWN'
        END AS chamber,
        g.member_name,
        CASE 
            WHEN g.party = 'R' THEN 'Republican'
            WHEN g.party = 'D' THEN 'Democrat'
            ELSE 'UNKNOWN'
        END AS party,
        IFNULL(s.state_name, 'UNKNOWN') AS state,
        IFNULL(s.state_ISO_FORMAT, 'UNKNOWN') AS state_iso_format,
        g.district,
        g.committees,
        g.leadership_positions,
        TO_TIMESTAMP_TZ(g.member_updated) AS member_updated_utc,
        ROW_NUMBER() OVER (PARTITION BY g.member_id ORDER BY g.member_updated DESC) AS row_num
    FROM gov_officials AS g
    LEFT JOIN states_CTE AS s
        ON s.state_abbrev = g.state
    {% if is_incremental() %}
    WHERE report_date = '{{ var("run_date") }}'
    {% endif %}
)
SELECT
member_id,
chamber,
member_name,
party,
state,
state_iso_format,
district,
committees,
leadership_positions,
member_updated_utc
FROM deduped
WHERE row_num=1