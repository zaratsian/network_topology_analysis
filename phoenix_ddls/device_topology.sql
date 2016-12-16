--/phoenix/bin/psql.py -t DEVICE_TOPOLOGY localhost /traceroute_google_mapped.csv
CREATE TABLE IF NOT EXISTS DEVICE_TOPOLOGY (
    topology VARCHAR NOT NULL PRIMARY KEY);
