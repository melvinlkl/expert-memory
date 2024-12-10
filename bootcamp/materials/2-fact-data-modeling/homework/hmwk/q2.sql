-- Q2: A DDL for an user_devices_cumulated table
CREATE TABLE users_devices_cumulated (
    user_id TEXT,
    device_id TEXT,
    -- list of dates in the past where device was active
    device_activity_datelist DATE[],
    browser_type TEXT,
    cur_date DATE,
    PRIMARY KEY (user_id, device_id, browser_type)
);
