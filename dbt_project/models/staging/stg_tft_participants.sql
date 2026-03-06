{{ config(materialized='view') }}

WITH raw AS (
    SELECT * FROM {{ source('tft_raw', 'raw_matches') }}
),
stg_matches AS (
    SELECT
        match_id,
        tft_set_number
    FROM {{ ref('stg_tft_matches') }}
),
participants AS (
    SELECT
        m.match_id,
        jsonb_array_elements(m.match_info::jsonb->'info'->'participants') AS participant_json
    FROM raw m
    INNER JOIN stg_matches sm
        ON m.match_id = sm.match_id
    WHERE sm.tft_set_number = 16
)

SELECT
    match_id,
    participant_json->>'puuid' AS puuid,
    CAST(participant_json->>'placement' AS INTEGER) AS placement,
    CAST(participant_json->>'level' AS INTEGER) AS level,
    CAST(participant_json->>'gold_left' AS INTEGER) AS gold_left,
    CAST(participant_json->>'time_eliminated' AS FLOAT) AS time_eliminated,
    CAST(participant_json->>'total_damage_to_players' AS INTEGER) AS total_damage_to_players
FROM participants
