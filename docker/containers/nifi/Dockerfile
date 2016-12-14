FROM base
ARG NIFI_URL=https://archive.apache.org/dist/nifi/1.0.0/nifi-1.0.0-bin.tar.gz
ARG NIFI_VER=nifi-1.0.0

RUN wget ${NIFI_URL} -O /nifi.tgz
RUN tar -xzvf /nifi.tgz
RUN mv ${NIFI_VER} /nifi

ADD resources /resources
#ARG PHOENIX_JDBC_URL=http://central.maven.org/maven2/org/apache/phoenix/phoenix-queryserver-client/4.8.1-HBase-1.1/phoenix-queryserver-client-4.8.1-HBase-1.1.jar
#RUN wget ${PHOENIX_JDBC_URL} -O /resources/phoenix-queryserver-client.jar

RUN mv /resources/flow.xml.gz /nifi/conf/
CMD source /root/.bashrc; /nifi/bin/nifi.sh start; tail -f /nifi/logs/nifi-app.log
