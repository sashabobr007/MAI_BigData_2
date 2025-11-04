FROM spark:python3

USER root


# Установка Python зависимостей
RUN pip install redis neo4j pymongo cassandra-driver

# Копирование JAR файлов
COPY jars/ /opt/spark/jars/