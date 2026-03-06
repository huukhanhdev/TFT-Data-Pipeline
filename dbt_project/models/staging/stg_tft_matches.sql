{{ config(materialized='view') }}

WITH raw AS (
    SELECT * FROM {{ source('tft_raw', 'raw_matches') }}
)

SELECT
    match_id,
    data_version,
    CAST(match_info::jsonb->'info'->>'game_datetime' AS BIGINT) AS game_datetime,
    CAST(match_info::jsonb->'info'->>'game_length' AS FLOAT) AS game_length,
    match_info::jsonb->'info'->>'game_version' AS game_version,
    CAST(match_info::jsonb->'info'->>'tft_set_number' AS INTEGER) AS tft_set_number,
    match_info::jsonb->'info'->>'tft_set_name' AS tft_set_name,
    fetched_at
FROM raw
