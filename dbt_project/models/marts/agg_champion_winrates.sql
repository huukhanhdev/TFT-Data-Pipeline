{{ config(materialized='table') }}

WITH unit_placements AS (
    SELECT 
        u.character_id,
        u.star_level,
        p.placement,
        CASE WHEN p.placement <= 4 THEN 1 ELSE 0 END AS is_top_4,
        CASE WHEN p.placement = 1 THEN 1 ELSE 0 END AS is_first
    FROM {{ ref('stg_tft_units') }} u
    JOIN {{ ref('stg_tft_participants') }} p 
        ON u.match_id = p.match_id AND u.puuid = p.puuid
),
stats AS (
    SELECT 
        character_id,
        star_level,
        COUNT(*) AS total_games_played,
        SUM(is_top_4) AS total_top_4,
        SUM(is_first) AS total_firsts,
        ROUND(CAST(SUM(is_top_4) AS NUMERIC) / COUNT(*), 4) AS top_4_rate,
        ROUND(CAST(SUM(is_first) AS NUMERIC) / COUNT(*), 4) AS first_place_rate,
        ROUND(AVG(placement), 2) AS avg_placement
    FROM unit_placements
    GROUP BY 
        character_id,
        star_level
)

SELECT 
    u.character_id,
    d.champion_name,
    d.cost,
    u.star_level,
    u.total_games_played,
    u.total_top_4,
    u.total_firsts,
    u.top_4_rate,
    u.first_place_rate,
    u.avg_placement
FROM stats u
LEFT JOIN {{ ref('dim_champions') }} d ON u.character_id = d.character_id
WHERE u.total_games_played > 5
ORDER BY u.top_4_rate DESC
