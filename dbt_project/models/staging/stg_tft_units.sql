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
units AS (
    SELECT
        match_id,
        participant_json->>'puuid' AS puuid,
        jsonb_array_elements(participant_json->'units') AS unit_json
    FROM participants
)

SELECT
    match_id,
    puuid,
    unit_json->>'character_id' AS character_id,
    CAST(unit_json->>'rarity' AS INTEGER) AS rarity,
    CAST(unit_json->>'tier' AS INTEGER) AS star_level
FROM units
