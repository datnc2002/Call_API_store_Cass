FROM bitnami/spark:3.4.1

USER root

RUN apt-get update && apt-get install -y wget && \
    wget https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3-scala2.12.tgz && \
    tar -xzf spark-3.4.1-bin-hadoop3-scala2.12.tgz && \
    mv spark-3.4.1-bin-hadoop3-scala2.12 /opt/bitnami/spark && \
    rm spark-3.4.1-bin-hadoop3-scala2.12.tgz && \
    apt-get clean && \
    pip install py4j==0.10.9.7


USER 1001

