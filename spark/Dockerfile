FROM spark:3.5.8-python3

USER root

RUN mkdir -p /opt/spark/jars

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

ADD https://jdbc.postgresql.org/download/postgresql-42.7.8.jar /opt/spark/jars/postgresql.jar

USER 185