The intent of this project is to:

1. Provide a light, single-container [Apache Phoenix](http://phoenix.apache.org/) service for dev/testing
2. Expose [thin-client](http://phoenix.apache.org/server.html) Phoenix service endpoint

The container also runs [Apache HBase](http://hbase.apache.org/), configured for local storage instead of HDFS.

Build:
```
docker build -t phoenix .
```

Run with only Query Server port exposed
```
docker run -d -p 8765:8765 phoenix
```

Errata:
HMaster/ZooKeeper advertises the container's hostname as the RegionServer address. This is ok for Docker running in a VM with dedicated IP.

This does *not* work on Docker for Mac.

Run exposing HBase ports for thick JDBC client
```
docker run -d -p 8765:8765 -p 2181:2181 -p 16010:16010 -p 16020:16020 -p 16030:16030 --hostname phoenix phoenix
```

Add container hostname to /etc/hosts
```
sudo echo "127.0.0.1  phoenix" >> /etc/hosts
```
