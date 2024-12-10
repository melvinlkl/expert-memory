-- Q7: A monthly, reduced fact table DDL host_activity_reduced - month, host, hit_array, unique_visitor_array
CREATE TABLE host_activity_reduced (
    user_id TEXT,
    month DATE,
    host TEXT,
    hit_array REAL[],
    unique_visitor_array REAL[],
    PRIMARY KEY (user_id, month, host)
);