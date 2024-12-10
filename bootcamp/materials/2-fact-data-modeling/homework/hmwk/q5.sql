-- Q5: DDL for hosts_cumulated table
CREATE TABLE hosts_cumulated (
    user_id TEXT,
    host_activity_datelist DATE[],
    cur_date DATE,
    PRIMARY KEY (user_id)
);