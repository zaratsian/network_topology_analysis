FROM base
ARG ZEPPELIN_URL=https://archive.apache.org/dist/zeppelin/zeppelin-0.6.2/zeppelin-0.6.2-bin-all.tgz
ARG ZEPPELIN_VER=zeppelin-0.6.2-bin-all

ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-2.0.1/spark-2.0.1-bin-hadoop2.7.tgz
ARG SPARK_VER=spark-2.0.1-bin-hadoop2.7

RUN wget ${SPARK_URL} -O /spark.tgz
RUN tar -xzvf spark.tgz
RUN mv ${SPARK_VER} /spark

RUN wget ${ZEPPELIN_URL} -O /zeppelin.tgz
RUN tar -xzvf zeppelin.tgz
RUN mv ${ZEPPELIN_VER} /zeppelin
RUN echo "export SPARK_HOME=/spark" >> /zeppelin/conf/zeppelin-env.sh

ADD resources /resources
RUN mv /resources/interpreter.json /zeppelin/conf/
RUN rm -rf /zeppelin/notebook
RUN mv /resources/notebook/ /zeppelin/notebook/
CMD source /root/.bashrc; cd /spark; /zeppelin/bin/zeppelin.sh run
