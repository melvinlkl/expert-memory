CREATE TABLE actors_history_scd (
    actor TEXT,
    actorid TEXT,
    quality_class QUALITY_CLASS,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    year INTEGER,
    PRIMARY KEY (actor,actorid,year)

);