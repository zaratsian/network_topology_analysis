CREATE TABLE IF NOT EXISTS DEVICE_TOPOLOGY (
    ip CHAR(15) NOT NULL,
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
    device_health INTEGER,
    upstream_device VARCHAR,
    node_path VARCHAR
    CONSTRAINT pk PRIMARY KEY (ip, upstream_device));
