-- Q3: A cumulative query to generate device_activity_datelist from events
WITH yesterday AS (
    SELECT
        *
    FROM
        users_devices_cumulated
    WHERE cur_date = DATE('2022-12-31')
),
    today AS (
    SELECT
        CAST(user_id AS TEXT),
        CAST(e.device_id AS TEXT) AS device_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS device_activity_datelist,
        browser_type,
        event_time
    FROM
        events e
    INNER JOIN devices d
    ON e.device_id = d.device_id
),
    today_filtered AS (
     SELECT
         user_id,
         device_id,
         device_activity_datelist,
         browser_type
     FROM today
     WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
    AND user_id IS NOT NULL
    GROUP BY user_id, device_id, device_activity_datelist, browser_type

)

INSERT INTO users_devices_cumulated
SELECT
COALESCE(t.user_id, y.user_id) AS user_id,
COALESCE(t.device_id, y.device_id) AS device_id,
CASE WHEN y.device_activity_datelist IS NULL
     THEN ARRAY[t.device_activity_datelist]
     WHEN t.device_activity_datelist IS NULL
     THEN y.device_activity_datelist
     ELSE ARRAY[t.device_activity_datelist] || y.device_activity_datelist
END
    AS device_activity_datelist,
COALESCE(t.browser_type, y.browser_type) AS browser_type,
COALESCE(t.device_activity_datelist, y.cur_date + INTERVAL '1 day') AS cur_date
FROM today_filtered t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
AND t.device_id = y.device_id
AND t.browser_type = y.browser_type;