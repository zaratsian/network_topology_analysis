docker stop phoenix zeppelin nifi kafka
docker rm -f phoenix zeppelin nifi kafka

docker run -d -p 8765:8765 --hostname phoenix --net dev --name phoenix phoenix
docker run -d -p 8079:8080 -p 4040:4040 --hostname zeppelin --net dev --name zeppelin zeppelin
docker run -d -p 8080:8080 --hostname nifi --net dev --name nifi nifi
docker run -d --hostname kafka --net dev --name kafka kafka

# Zeppelin and Spark
# Usage:
# /spark/bin/spark-submit --master local[*] --class "cxStream" --jars /phoenix-spark-4.8.1-HBase-1.1.jar target/SparkStreaming-0.0.1.jar phoenix.dev:2181 mytestgroup dztopic1 1 kafka.dev:9092
docker cp assets/mosaic_topology_mapped.csv zeppelin:/mosaic_topology_mapped.csv
docker cp ../SparkCx zeppelin:/SparkCx
docker cp assets/log4j.properties zeppelin:/spark/conf/log4j.properties
docker exec zeppelin wget https://archive.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
docker exec zeppelin tar xvf apache-maven-3.3.9-bin.tar.gz
docker exec zeppelin wget http://central.maven.org/maven2/org/apache/phoenix/phoenix-spark/4.8.1-HBase-1.1/phoenix-spark-4.8.1-HBase-1.1.jar


# Phoenix


# Kafka
docker cp assets/mosaic_data.csv kafka:/mosaic_data.csv
docker cp assets/mosaic_data_smalltest.csv kafka:/mosaic_data_smalltest.csv
docker cp assets/stream_kafka.py kafka:/stream_kafka.py
docker cp assets/stream_kafka_smalltest.py kafka:/stream_kafka_smalltest.py
docker exec kafka /kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic dztopic1
docker exec kafka /kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list
docker exec kafka curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
docker exec kafka python get-pip.py
docker exec kafka pip install kafka


# NiFi


# Completion
echo "***************************************************"
echo "*"
echo "*  Start-up Complete!"
echo "*"
echo "*  Port (NiFi):     8080"
echo "*  Port (Zeppelin): 8079"
echo "*  Port (Phoenix):  8765"
echo "*"
echo "*  Usage: docker exec -it <container> bash"
echo "*"
echo "***************************************************"

#ZEND
