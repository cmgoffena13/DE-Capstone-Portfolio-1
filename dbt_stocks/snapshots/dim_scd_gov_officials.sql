{% snapshot dim_scd_gov_officials %}
{{ config(
    target_schema='public',
    unique_key='member_id',
    strategy='timestamp',
    updated_at='member_updated_utc',
    invalidate_hard_deletes=True
) }}

SELECT
    member_id,
    chamber,
    member_name,
    party,
    state,
    district,
    committees,
    leadership_positions,
    member_updated_utc
FROM {{ ref('int_gov_officials') }} AS g

{% endsnapshot %}