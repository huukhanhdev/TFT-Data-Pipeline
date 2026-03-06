{{ config(materialized='table') }}

WITH filtered_matches AS (
    SELECT * FROM {{ ref('stg_tft_matches') }}
    WHERE tft_set_number = 16
)

SELECT 
    m.match_id,
    m.game_datetime,
    m.game_length,
    m.game_version,
    m.tft_set_name,
    p.puuid,
    p.placement,
    p.level,
    p.gold_left,
    p.time_eliminated,
    p.total_damage_to_players
FROM filtered_matches m
JOIN {{ ref('stg_tft_participants') }} p ON m.match_id = p.match_id
