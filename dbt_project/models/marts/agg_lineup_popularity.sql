{{ config(materialized='table') }}

WITH unit_list AS (
    SELECT
        match_id,
        puuid,
        string_agg(character_id, ', ' ORDER BY character_id) AS lineup_signature
    FROM {{ ref('stg_tft_units') }}
    GROUP BY 1, 2
),
match_results AS (
    SELECT
        u.lineup_signature,
        p.placement
    FROM unit_list u
    JOIN {{ ref('stg_tft_participants') }} p
      ON u.match_id = p.match_id AND u.puuid = p.puuid
)

SELECT
    lineup_signature,
    COUNT(*) AS play_count,
    ROUND(AVG(placement), 2) AS avg_placement,
    COUNT(CASE WHEN placement <= 4 THEN 1 END) AS top_4_count,
    ROUND(COUNT(CASE WHEN placement <= 4 THEN 1 END) * 100.0 / COUNT(*), 2) AS top_4_rate
FROM match_results
GROUP BY 1
ORDER BY play_count DESC
