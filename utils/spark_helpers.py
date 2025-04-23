import os
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from utils.logger import logger


def execute_query(spark: SparkSession, path: str) -> DataFrame:
    try:
        with open(path, "r", encoding="utf-8") as f:
            query = f.read()
        logger.info(f"Ejecutando query SQL desde archivo: {path}")
        print(f"Contenido de la query:\n{query}")
        return spark.sql(query)
    except Exception as e:
        logger.exception("Error al ejecutar la consulta SQL.")
        raise


def load_parquet(spark: SparkSession, path: str) -> DataFrame:
    try:
        logger.info(f"Cargando datos desde Parquet: {path}")
        return spark.read.parquet(path)
    except Exception as e:
        logger.exception(f"Error al cargar el archivo Parquet: {path}")
        raise


def save_parquet(data: DataFrame, path: str, mode: str = "overwrite", repartition: int = 4):

    try:
        Path(path).mkdir(parents=True, exist_ok=True)
        data.repartition(repartition).write.mode(mode).parquet(path)
        logger.info(f"DataFrame guardado exitosamente en: {path}")
    except Exception as e:
        logger.exception(f"Error al guardar DataFrame en: {path}")
        raise


def register_temp_view(df: DataFrame, name: str):
    try:
        df.createOrReplaceTempView(name)
        logger.info(f"Vista temporal registrada: {name}")
    except Exception as e:
        logger.exception(f"Error al registrar la vista temporal: {name}")
        raise
