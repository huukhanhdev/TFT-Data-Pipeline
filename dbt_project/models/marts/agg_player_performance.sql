{{ config(materialized='table') }}

SELECT
    placement,
    COUNT(*) AS total_games,
    ROUND(AVG(level), 2) AS avg_final_level,
    ROUND(AVG(gold_left), 2) AS avg_gold_left,
    ROUND(AVG(total_damage_to_players), 2) AS avg_damage_dealt
FROM {{ ref('stg_tft_participants') }}
GROUP BY placement
ORDER BY placement ASC
