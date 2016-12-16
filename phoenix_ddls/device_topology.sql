--/phoenix/bin/psql.py -t device_topology localhost /traceroute_google_mapped.txt
CREATE TABLE IF NOT EXISTS DEVICE_TOPOLOGY (
    topology VARCHAR NOT NULL PRIMARY KEY);
