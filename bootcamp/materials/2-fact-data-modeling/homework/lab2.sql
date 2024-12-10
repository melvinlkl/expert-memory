CREATE TABLE users_cumulated (
    user_id TEXT,
    -- list of dates in the past where user was active
    date_active DATE[],
    cur_date DATE,
    PRIMARY KEY (user_id, cur_date)
);

WITH yesterday AS (
    SELECT
        *
    FROM
        users_cumulated
    WHERE cur_date = DATE('2022-12-31')
),
    today AS (
    SELECT
        CAST(user_id AS TEXT),
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM
        events
    WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
    AND user_id IS NOT NULL
    GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)

SELECT
COALESCE(t.user_id, y.user_id),
CASE WHEN y.date_active IS NULL
     THEN ARRAY[]
     WHEN t.date_active IS NULL
     THEN y.date_active
     ELSE t.date_active || y.date_active
END
    AS date_active,
COALESCE(t.date_active, y.cur_date + INTERVAL '1 day') AS cur_date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id


