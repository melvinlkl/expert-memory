WITH users AS (
    -- list of active dates in this table
    SELECT *
    FROM users_cumulated
    WHERE cur_date = DATE('2023-01-31')

), series AS (
  SELECT *
  FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),
    place_holder_int AS (
    SELECT
        user_id,
       CASE WHEN dates_active @> ARRAY[DATE(series_date)]
        -- if active today, date - DATE(series_date) = 0 => 2^32
        -- converts date into bit values
        THEN CAST(POW(2,32 - (date - DATE(series_date))) AS BIGINT)
        ELSE 0
    END AS placeholder_int_value,
        *
    FROM users
    CROSS JOIN series
)

SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
    BIT_COUNT((CAST('11111110000000000000000000000000' AS BIT(32)) & (CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))))) > 0 AS dim_is_weekly_active
FROM place_holder_int
GROUP BY user_id