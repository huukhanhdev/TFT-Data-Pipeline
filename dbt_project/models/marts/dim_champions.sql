{{ config(materialized='table') }}

WITH mapping AS (
    SELECT * FROM {{ ref('champion_mapping') }}
),
stg_units AS (
    SELECT DISTINCT character_id FROM {{ ref('stg_tft_units') }}
)

SELECT 
    s.character_id,
    COALESCE(m.champion_name_vn, s.character_id) AS champion_name,
    COALESCE(m.cost, 0) AS cost
FROM stg_units s
LEFT JOIN mapping m ON s.character_id = m.character_id
