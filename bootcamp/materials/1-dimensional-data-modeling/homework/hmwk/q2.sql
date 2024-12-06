WITH last_year AS (
        SELECT actor,
               actorid,
               year,
               film,
               quality_class
        FROM actors
             WHERE year = 1969
        GROUP BY actor, actorid, year
),
    this_year AS (
        SELECT actor,
               actorid,
               year,
               ARRAY_AGG(ROW(
                    film,
                    votes,
                    rating,
                    filmid
                    )::films)
             AS film,
            CASE WHEN avg(rating) > 8 THEN 'star'
                 WHEN avg(rating) > 7 AND avg(rating)  <= 8 THEN 'good'
                 WHEN avg(rating) > 6 AND avg(rating)  <= 7 THEN 'average'
                 WHEN avg(rating) <= 6 THEN 'bad'
                 END::quality_class AS quality_class
        FROM actor_films
            WHERE year = 1970
            GROUP BY actor, actorid, year
)

INSERT INTO actors


SELECT
COALESCE(t.actor, l.actor) AS actor,
COALESCE(t.actorid, l.actorid) AS actorid,
CASE WHEN l.film IS NULL THEN t.film::films[]
    ELSE ARRAY_CAT(l.film::films[], t.film::films[])
    END AS films,
COALESCE(t.quality_class,l.quality_class) AS quality_class,
CASE WHEN t.film IS NOT NULL AND ARRAY_LENGTH(t.film,1) > 0 THEN TRUE
ELSE FALSE
END AS is_active,
CASE WHEN t.year IS NULL THEN l.year + 1
    ELSE t.year
END AS year
FROM this_year t FULL OUTER JOIN last_year l
ON t.actorid = l.actorid;