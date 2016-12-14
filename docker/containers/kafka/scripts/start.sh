source /root/.bashrc

./kafka/bin/zookeeper-server-start.sh /kafka/config/zookeeper.properties &
./kafka/bin/kafka-server-start.sh /kafka/config/server.properties
