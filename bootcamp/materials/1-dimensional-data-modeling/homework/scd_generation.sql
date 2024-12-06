CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY(player_name, start_season)
);

WITH streak_started AS (
    SELECT player_name,
           current_season,
           is_active,
           scoring_class,
           LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) <> scoring_class
               OR LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) IS NULL
               AS prev_scoring_class,
          LAG(is_active,1) OVER (PARTITION BY player_name ORDER by current_season) AS prev_is_active
    FROM players
),
    indicator AS (
        SELECT *,
            CASE WHEN scoring_class <> prev_scoring_class THEN 1
                 WHEN is_active <> prev_is_active THEN 1
                 ELSE 0
            END AS change_indicator
        FROM streak_started
),
     streak_identified AS (
         SELECT
            player_name,
            scoring_class,
            is_active,
            current_season,
            SUM(CASE WHEN change_indicator THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
         FROM indicator
     ),
     aggregated AS (
         SELECT
            player_name,
            scoring_class,
            streak_identifier,
            is_active,
            MIN(current_season) AS start_season,
            MAX(current_season) AS end_season
         FROM streak_identified
         GROUP BY 1,2,3,4
     )
    INSERT INTO players_scd
     SELECT player_name, scoring_class, is_active, start_season, end_season
     FROM aggregated