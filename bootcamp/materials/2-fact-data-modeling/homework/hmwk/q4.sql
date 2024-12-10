-- Q4:
WITH starter AS (
    SELECT uc.device_activity_datelist @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-01') - d.valid_date) AS days_since,
           uc.user_id
    FROM users_devices_cumulated uc
             CROSS JOIN
         (SELECT generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS valid_date) as d
    WHERE cur_date = DATE('2023-01-01')
),
     bits AS (
         SELECT user_id,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,
                DATE('2023-01-01') as cur_date
         FROM starter
         GROUP BY user_id
     )

     INSERT INTO datelist_int
     SELECT * FROM bits