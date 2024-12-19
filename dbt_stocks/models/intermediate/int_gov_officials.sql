WITH gov_officials AS (
    SELECT * FROM {{ ref('src_gov_official_trades') }}
),
states_CTE AS (
    SELECT * FROM {{ ref('states') }}
)
SELECT DISTINCT
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
    TO_TIMESTAMP_TZ(g.member_updated) AS member_updated_utc
FROM gov_officials AS g
LEFT JOIN states_CTE AS s
    ON s.state_abbrev = g.state