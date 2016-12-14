FROM base

ARG KAFKA_URL=https://archive.apache.org/dist/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz
ARG KAFKA_VER=kafka_2.10-0.8.2.1

RUN wget ${KAFKA_URL} -O /kafka.tgz
RUN tar -xzvf /kafka.tgz
RUN mv ${KAFKA_VER} /kafka

ADD scripts /scripts
CMD sh /scripts/start.sh


