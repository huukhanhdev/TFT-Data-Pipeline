{{ config(materialized='table') }}

WITH mapping AS (
    SELECT * FROM {{ ref('trait_mapping') }}
),
stg_traits AS (
    SELECT DISTINCT trait_id FROM {{ ref('stg_tft_traits') }}
)

SELECT 
    s.trait_id,
    COALESCE(m.trait_name_vn, s.trait_id) AS trait_name
FROM stg_traits s
LEFT JOIN mapping m ON s.trait_id = m.trait_id
