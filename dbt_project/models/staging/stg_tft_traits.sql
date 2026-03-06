{{ config(materialized='view') }}

WITH raw AS (
    SELECT * FROM {{ source('tft_raw', 'raw_matches') }}
),
stg_matches AS (
    SELECT match_id FROM {{ ref('stg_tft_matches') }} WHERE tft_set_number = 16
),
participants AS (
    SELECT
        m.match_id,
        jsonb_array_elements(m.match_info::jsonb->'info'->'participants') AS participant_json
    FROM raw m
    INNER JOIN stg_matches sm ON m.match_id = sm.match_id
),
traits AS (
    SELECT
        match_id,
        participant_json->>'puuid' AS puuid,
        jsonb_array_elements(participant_json->'traits') AS trait_json
    FROM participants
)

SELECT
    match_id,
    puuid,
    trait_json->>'name' AS trait_id,
    CAST(trait_json->>'num_units' AS INTEGER) AS num_units,
    CAST(trait_json->>'tier_current' AS INTEGER) AS tier_current,
    CAST(trait_json->>'tier_total' AS INTEGER) AS tier_total
FROM traits
WHERE CAST(trait_json->>'tier_current' AS INTEGER) > 0
