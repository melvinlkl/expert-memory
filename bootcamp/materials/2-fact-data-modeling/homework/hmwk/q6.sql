-- Q6: incremental query to generate host_activity_datelist
WITH yesterday AS (
    SELECT
        *
    FROM
        hosts_cumulated
    WHERE cur_date = DATE('2022-12-31')
),
    today AS (
    SELECT
        CAST(user_id AS TEXT),
        DATE(CAST(event_time AS TIMESTAMP)) AS host_activity_datelist,
        cur_date
    FROM
        events
     WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
        AND user_id IS NOT NULL
)


INSERT INTO hosts_cumulated
SELECT
COALESCE(t.user_id, y.user_id) AS user_id,
CASE WHEN y.host_activity_datelist IS NULL
     THEN ARRAY[t.host_activity_datelist]
     WHEN t.host_activity_datelist IS NULL
     THEN y.host_activity_datelist
     ELSE ARRAY[t.host_activity_datelist] || y.host_activity_datelist
END
    AS host_activity_datelist,
COALESCE(t.host_activity_datelist, y.cur_date + INTERVAL '1 day') AS cur_date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
