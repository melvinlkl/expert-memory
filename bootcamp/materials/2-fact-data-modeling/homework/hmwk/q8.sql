-- Q8: An incremental query that loads host_activity_reduced - day by day
WITH daily_aggregrate AS (
    SELECT
        CAST(user_id AS TEXT),
        host,
        DATE(event_time) AS cur_date,
        COUNT(1) AS site_hits,
        COUNT(distinct user_id) AS unique_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01')
    AND user_id IS NOT NULL
    GROUP BY user_id, host, DATE(event_time)
),
    yesterday_array AS (
        SELECT *
        FROM host_activity_reduced
        WHERE month = DATE('2023-01-01')
)

INSERT INTO host_activity_reduced
SELECT
    -- Select user_id from either daily_aggregate or yesterday_array
    COALESCE(da.user_id, ya.user_id) AS user_id,
    -- Determine month
    COALESCE(ya.month, DATE_TRUNC('month', da.cur_date)) AS month,
    COALESCE(da.host, ya.host) AS host,
    -- Update hit_array based on existing data and new daily aggregates
    CASE WHEN ya.hit_array IS NOT NULL THEN
        ya.hit_array || ARRAY[COALESCE(da.site_hits,0)]
        WHEN ya.hit_array IS NULL THEN
        ARRAY_FILL(0, ARRAY[COALESCE(cur_date - DATE(DATE_TRUNC('month',cur_date)),0)]) || ARRAY[COALESCE(da.site_hits,0)]
    END AS hit_array,
    -- Update unique_visitor_array based on existing data and new daily aggregates
    CASE WHEN ya.unique_visitor_array IS NOT NULL THEN
        ya.unique_visitor_array || ARRAY[COALESCE(da.unique_hits,0)]
        WHEN ya.unique_visitor_array IS NULL THEN
        ARRAY_FILL(0, ARRAY[COALESCE(cur_date - DATE(DATE_TRUNC('month',cur_date)),0)]) || ARRAY[COALESCE(da.unique_hits,0)]
    END AS unique_visitor_array
FROM daily_aggregrate da
FULL OUTER JOIN yesterday_array ya
ON da.user_id = ya.user_id
AND da.host = ya.host

ON CONFLICT (user_id, host, month)
DO
    UPDATE SET hit_array = EXCLUDED.hit_array,
               unique_visitor_array = EXCLUDED.unique_visitor_array;