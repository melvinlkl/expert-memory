CREATE TYPE scd_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_date INTEGER,
                    end_date INTEGER
                        );

WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE year = 1969
    AND year = 1969
),
     historical_scd AS (
        SELECT
            actor,
            actorid,
               quality_class,
               is_active,
               start_date,
               end_date
        FROM actors_history_scd
        WHERE year = 1969
        AND end_date < 1969
     ),
     this_year_data AS (
         SELECT * FROM actors
         WHERE year = 1970
     ),
     unchanged_records AS (
         SELECT
                t.actor,
                t.actorid,
                t.quality_class,
                t.is_active,
                l.start_date,
                t.year as end_date
        FROM this_year_data t
        JOIN last_year_scd l
        ON l.actorid = t.actorid
         WHERE t.quality_class = l.quality_class
         AND t.is_active = l.is_active
     ),
     changed_records AS (
        SELECT
                t.actor,
                t.actorid,
                UNNEST(ARRAY[
                    ROW(
                        l.quality_class,
                        l.is_active,
                        l.start_date,
                        l.end_date

                        )::scd_type,
                    ROW(
                        t.quality_class,
                        t.is_active,
                        t.year,
                        t.year
                        )::scd_type
                ]) as records
        FROM this_year_data t
        LEFT JOIN last_year_scd l
        ON l.actorid = t.actorid
         WHERE (t.quality_class <> l.quality_class
          OR t.is_active <> l.is_active)
     ),
     unnested_changed_records AS (

         SELECT actorid,
                actor,
                (records::scd_type).quality_class,
                (records::scd_type).is_active,
                (records::scd_type).start_date,
                (records::scd_type).end_date
                FROM changed_records
         ),
     new_records AS (

         SELECT
            t.actorid,
            t.actor,
                t.quality_class,
                t.is_active,
                t.year AS start_date,
                t.year AS end_date
         FROM this_year_data t
         LEFT JOIN last_year_scd l
             ON t.actorid = l.actorid
         WHERE l.actorid IS NULL

     )

INSERT INTO actors_history_scd

SELECT *, 1970 AS year FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records) a