{{ config(materialized='table') }}

WITH players AS (
    SELECT 
        puuid,
        MAX(tier) as tier,
        MAX(_rank) as rank,
        MAX(leaguepoints) as league_points
    FROM {{ source('tft_raw', 'raw_top_players') }}
    GROUP BY puuid
)

SELECT 
    puuid,
    tier,
    rank,
    league_points,
    tier || ' ' || rank AS full_rank
FROM players
