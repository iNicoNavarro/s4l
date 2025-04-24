# spark_docker/jupyter/Dockerfile

FROM jupyter/pyspark-notebook:latest

USER root
RUN apt-get update && apt-get install -y unixodbc-dev
USER jovyan

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN python -m ipykernel install --user --name pyspark --display-name "PySpark"

# Copia el driver JDBC de SQL Server
COPY spark_docker/jupyter/drivers/mssql-jdbc-12.10.0.jre11.jar /usr/local/spark/jars/
