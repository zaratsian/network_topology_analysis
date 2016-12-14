FROM base
ARG HBASE_URL=https://archive.apache.org/dist/hbase/1.1.7/hbase-1.1.7-bin.tar.gz
ARG PHOENIX_URL=https://archive.apache.org/dist/phoenix/apache-phoenix-4.8.1-HBase-1.1/bin/apache-phoenix-4.8.1-HBase-1.1-bin.tar.gz

ARG HBASE_VER=hbase-1.1.7
ARG PHOENIX_VER=4.8.1-HBase-1.1

# Setup HBase binaries
RUN wget -nv ${HBASE_URL} -O /hbase.tgz
RUN tar -xzvf /hbase.tgz
RUN mv /${HBASE_VER} /hbase

# Setup Phoenix binaries
RUN wget -nv ${PHOENIX_URL} -O /phoenix.tgz
RUN tar -xzvf /phoenix.tgz
RUN mv /apache-phoenix-${PHOENIX_VER}-bin /phoenix
RUN cp /phoenix/phoenix-${PHOENIX_VER}-server.jar /hbase/lib

ADD conf /hbase/conf
CMD source /root/.bashrc; sh /hbase/bin/start-hbase.sh; sleep 10; /phoenix/bin/queryserver.py
