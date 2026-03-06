{{ config(materialized='table') }}

WITH mapping AS (
    SELECT * FROM {{ ref('item_mapping') }}
),
stg_items AS (
    SELECT DISTINCT item_name FROM {{ ref('stg_tft_items') }}
)

SELECT 
    s.item_name AS item_id,
    COALESCE(m.item_name_vn, s.item_name) AS item_name,
    COALESCE(m.item_type, 'Khác') AS item_type
FROM stg_items s
LEFT JOIN mapping m ON s.item_name = m.item_id
