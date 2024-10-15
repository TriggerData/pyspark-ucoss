FROM continuumio/miniconda3


# Install Apache Spark & deps
ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
RUN apt-get update && apt-get install -y openjdk-17-jdk wget curl zip unzip && \
    wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy spark app & install dependencies
COPY . /opt/app
RUN pip install pyspark==3.5.2 delta-spark==3.2.0


# Download unitycatalog
RUN git clone https://github.com/unitycatalog/unitycatalog.git /opt/unitycatalog
WORKDIR /opt/unitycatalog

# Build & publish spark connector
RUN build/sbt clean package publishLocal spark/publishLocal

# Start unitycatalog server
CMD ["bin/start-uc-server"]
 