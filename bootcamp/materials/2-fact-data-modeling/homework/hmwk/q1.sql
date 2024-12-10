-- Q1: A query to deduplicate game_details from Day 1 so there's no duplicates
WITH dedup AS (SELECT
    gd.*,
    -- Partition by game_id, team_id, player_id ensures unique combination
    ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id) AS row_num
FROM game_details gd)

SELECT *
FROM dedup
WHERE row_num = 1
