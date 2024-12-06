WITH streak_started AS (
    SELECT actor,
           actorid,
           quality_class,
           is_active,
           LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY year) <> quality_class
               OR LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY year) IS NULL
               OR LAG(is_active,1) OVER (PARTITION BY actor ORDER by year) <> is_active
               OR LAG(is_active,1) OVER (PARTITION BY actor ORDER BY year) IS NULL AS change_indicator,
           year
    FROM actors
),
     streak_identified AS (
         SELECT
            actor,
            actorid,
            quality_class,
            is_active,
            year,
            SUM(CASE WHEN change_indicator THEN 1 ELSE 0 END)
                OVER (PARTITION BY actor ORDER BY year) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            actor,
            actorid,
            quality_class,
            streak_identifier,
            is_active,
            year,
            MIN(year) AS start_date,
            MAX(year) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3,4,5,6
     )
    INSERT INTO actors_history_scd
     SELECT actor, actorid, quality_class, is_active,
            start_date, end_date, year
     FROM aggregated