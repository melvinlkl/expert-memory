CREATE TYPE season_stats AS (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
    );

CREATE TYPE scoring_class AS ENUM ('star','good','average','bad');

CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    scoring_class scoring_class,
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY (player_name, current_season)
);

--Backfill Query
WITH yesterday AS (
        SELECT * FROM players
             WHERE current_season = 1997
),
    today AS (
        SELECT * FROM player_seasons
            WHERE season = 1998
)
INSERT INTO players
SELECT
COALESCE(t.player_name, y.player_name) AS player_name,
COALESCE(t.height, y.height) AS height,
COALESCE(t.college, y.college) AS college,
COALESCE(t.country, y.country) AS country,
COALESCE(t.draft_year, y.draft_year) AS draft_year,
COALESCE(t.draft_round, y.draft_round) AS draft_round,
COALESCE(t.draft_number, y.draft_number) AS draft_number,
COALESCE(y.season_stats,
            ARRAY[]::season_stats[]
            ) || CASE WHEN t.season IS NOT NULL THEN
                ARRAY[ROW(
                t.season,
                t.gp,
                t.pts,
                t.ast,
                t.reb)::season_stats]
                ELSE ARRAY[]::season_stats[] END
            as season_stats,
CASE WHEN t.season IS NOT NULL THEN
    CASE WHEN t.pts > 20 THEN 'star'
         WHEN t.pts > 15 THEN 'good'
         WHEN t.pts > 10 THEN 'average'
         ELSE 'bad'
    END::scoring_class
    ELSE y.scoring_class
END AS scoring_class,
1998 AS current_season,
CASE WHEN t.season IS NOT NULL
    THEN TRUE
    ELSE FALSE
END AS is_active
FROM today t FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name;