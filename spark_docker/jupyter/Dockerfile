# spark_docker/jupyter/Dockerfile

FROM jupyter/pyspark-notebook:latest


COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN python -m ipykernel install --user --name pyspark --display-name "PySpark"
