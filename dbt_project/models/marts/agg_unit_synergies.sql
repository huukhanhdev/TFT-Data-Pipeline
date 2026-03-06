{{ config(materialized='table') }}

WITH unit_pairs AS (
    SELECT
        u1.match_id,
        u1.character_id AS unit_a,
        u2.character_id AS unit_b,
        p.placement
    FROM {{ ref('stg_tft_units') }} u1
    JOIN {{ ref('stg_tft_units') }} u2 
        ON u1.match_id = u2.match_id AND u1.puuid = u2.puuid
    JOIN {{ ref('stg_tft_participants') }} p
        ON u1.match_id = p.match_id AND u1.puuid = p.puuid
    WHERE u1.character_id < u2.character_id -- Prevent double counting (A,B and B,A)
)

SELECT
    unit_a,
    unit_b,
    COUNT(*) AS play_count,
    ROUND(CAST(AVG(placement) AS NUMERIC), 2) AS avg_placement,
    SUM(CASE WHEN placement <= 4 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS top_4_rate
FROM unit_pairs
GROUP BY 1, 2
HAVING COUNT(*) > 50
ORDER BY top_4_rate DESC
LIMIT 100
