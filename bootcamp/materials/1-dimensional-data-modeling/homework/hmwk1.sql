-- Q1

CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
                     );

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors AS (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    PRIMARY KEY(actor,actorid)
);

-- Q2
INSERT INTO actors
WITH yesterday AS (
    SELECT * FROM actor_films
             WHERE year = 1999
), today AS (
    SELECT * FROM actor_films
             WHERE year = 2000
)

SELECT
COALESCE(t.actor, y.actor) AS actor,
COALESCE(t.actorid, y.actorid) AS actorid,
CASE WHEN y.films IS NULL
    THEN ARRAY[ROW(
        t.film,
        t.votes,
        t.rating,
        t.filmid
        )]
    WHEN y.films IS NOT NULL
    THEN y.films || ARRAY[ROW(
        t.film,
        t.votes,
        t.rating,
        t.filmid
        )::films]
END AS films,
CASE WHEN t.year IS NOT NULL THEN
    CASE WHEN t.rating > 8 THEN 'star'
         WHEN t.rating > 7 AND t.rating <= 8 THEN 'good'
         WHEN t.rating > 6 AND t.rating <=7 THEN 'average'
         WHEN t.rating <= 6 THEN 'bad'
    END::quality_class
END AS quality_class,
COALESCE(t.year, y.year + 1) as current_year,
CASE WHEN t.year IS NOT NULL THEN TRUE
ELSE FALSE
END AS is_active
FROM today t FULL OUTER JOIN yesterday y
ON t.actorid = y.actorid;

-- Q3