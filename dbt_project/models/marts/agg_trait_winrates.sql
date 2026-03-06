{{ config(materialized='table') }}

WITH trait_results AS (
    SELECT
        t.trait_id,
        t.tier_current,
        p.placement,
        CASE WHEN p.placement <= 4 THEN 1 ELSE 0 END AS is_top_4,
        CASE WHEN p.placement = 1 THEN 1 ELSE 0 END AS is_win
    FROM {{ ref('stg_tft_traits') }} t
    JOIN {{ ref('stg_tft_participants') }} p 
        ON t.match_id = p.match_id AND t.puuid = p.puuid
),
stats AS (
    SELECT
        trait_id,
        tier_current,
        COUNT(*) AS play_count,
        ROUND(CAST(AVG(placement) AS NUMERIC), 2) AS avg_placement,
        ROUND(CAST(AVG(is_top_4) AS NUMERIC), 4) AS top_4_rate,
        ROUND(CAST(AVG(is_win) AS NUMERIC), 4) AS win_rate
    FROM trait_results
    GROUP BY 1, 2
)

SELECT
    s.trait_id,
    d.trait_name,
    s.tier_current,
    s.play_count,
    s.avg_placement,
    s.top_4_rate,
    s.win_rate
FROM stats s
LEFT JOIN {{ ref('dim_traits') }} d ON s.trait_id = d.trait_id
WHERE s.play_count > 10
ORDER BY s.trait_id, s.tier_current DESC
