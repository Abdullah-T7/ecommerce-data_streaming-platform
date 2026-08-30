FROM spark:3.5.8-python3

USER root

RUN mkdir -p /opt/spark/jars

ADD https://jdbc.postgresql.org/download/postgresql-42.7.8.jar /opt/spark/jars/postgresql.jar

USER 185
