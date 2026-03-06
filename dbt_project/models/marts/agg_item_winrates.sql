{{ config(materialized='table') }}

WITH item_results AS (
    SELECT
        i.item_name AS item_id,
        p.placement,
        p.puuid,
        CASE WHEN p.placement <= 4 THEN 1 ELSE 0 END AS is_top_4,
        CASE WHEN p.placement = 1 THEN 1 ELSE 0 END AS is_win
    FROM {{ ref('stg_tft_items') }} i
    JOIN {{ ref('stg_tft_participants') }} p 
        ON i.match_id = p.match_id 
        AND i.puuid = p.puuid
),
stats AS (
    SELECT
        item_id,
        COUNT(*) AS play_count,
        ROUND(CAST(AVG(placement) AS NUMERIC), 2) AS avg_placement,
        ROUND(CAST(AVG(is_top_4) AS NUMERIC), 4) AS top_4_rate,
        ROUND(CAST(AVG(is_win) AS NUMERIC), 4) AS win_rate
    FROM item_results
    GROUP BY 1
)

SELECT
    s.*,
    d.item_name,
    d.item_type
FROM stats s
LEFT JOIN {{ ref('dim_items') }} d ON s.item_id = d.item_id
WHERE s.play_count > 10
ORDER BY s.win_rate DESC, s.play_count DESC
