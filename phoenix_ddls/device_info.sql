CREATE TABLE IF NOT EXISTS DEVICE_INFO (
    ip CHAR(15) NOT NULL PRIMARY KEY,
    country VARCHAR,
    region VARCHAR,
    city VARCHAR,
    longitude VARCHAR,
    latitude VARCHAR,
    isp VARCHAR,
    org VARCHAR,
    level INTEGER,
    signal_strength FLOAT,
    signal_noise FLOAT,
    health_status INTEGER);
