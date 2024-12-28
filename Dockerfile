# Start from an Ubuntu base image
FROM ubuntu:22.04

# Install necessary packages including Spark dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget netcat && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download and install Spark
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt/ && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Set the working directory
WORKDIR $SPARK_HOME

# Start Spark master service
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
