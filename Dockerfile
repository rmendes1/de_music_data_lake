FROM python:3.11-bullseye as spark-base

ARG SPARK_VERSION=3.4.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*do

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN curl https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz -o spark-3.3.1-bin-hadoop3.tgz \
 && tar xvzf spark-3.3.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.3.1-bin-hadoop3.tgz

COPY spark_files/requirements.txt .
RUN pip install -r requirements.txt

COPY cred_key_gcloud.json /tmp/cred_key_gcloud.json

# Define a variável de ambiente para as credenciais do GCS
ENV GOOGLE_APPLICATION_CREDENTIALS=/tmp/cred_key_gcloud.json

# Instala o conector GCS para Hadoop (necessário para o Spark)
RUN curl -L -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

ENV SPARK_HOME="/opt/spark"
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
ENV SPARK_CONF_DIR=$SPARK_HOME/conf

COPY spark_files/jars/postgresql-42.6.2.jar $SPARK_HOME/jars

COPY spark_files/conf/spark-defaults.conf "$SPARK_CONF_DIR"
RUN echo "spark.hadoop.google.cloud.auth.service.account.enable true" >> $SPARK_CONF_DIR/spark-defaults.conf
RUN echo "spark.hadoop.google.cloud.auth.service.account.json.keyfile $GOOGLE_APPLICATION_CREDENTIALS" >> $SPARK_CONF_DIR/spark-defaults.conf
RUN echo "spark.hadoop.fs.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" >> $SPARK_CONF_DIR/spark-defaults.conf
RUN echo "spark.hadoop.fs.AbstractFileSystem.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" >> $SPARK_CONF_DIR/spark-defaults.conf

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]