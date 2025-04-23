import os
import sys
from pathlib import Path
import argparse
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import logger
from utils.spark_helpers import execute_query, load_parquet, register_temp_view


QUERY_PATH__ATENCION_DETALLE: str = 'puntos_scripts/queries/atencion_detalle.sql'
OUTPUT_PATH: str = "data_lake/gold/atencion_detalle"
STORAGE_LOCATION__SILVER_PATH: str = "data_lake/silver"
TABLE_NAMES: list = [
        "dim_patient",
        "dim_speciality",
        "dim_speciality_group",
        "dim_assignment_status",
        "dim_assistance_status",
        "fact_attention"
    ]


def load_dataframes(spark, storage_location: str, table_names:list):
    dfs = {}
    for name in table_names:
        dfs[name] = spark.read.parquet(f"{storage_location}/{name}")
    return dfs


# def register_temp_views(dfs):
#     for name, df in dfs.items():
#         df.createOrReplaceTempView(name)


# def execute_query(spark, query_path: str):
#     try:
#         with open(query_path, "r", encoding="utf-8") as file:
#             query = file.read()
#             logger.info(f"Ejecutando query desde: {query_path}")
#             print(f"Contenido de la query:\n{query}")
#             return spark.sql(query)
#     except AnalysisException as e:
#         logger.exception("Error de análisis en Spark SQL.")
#         raise
#     except Exception as e:
#         logger.exception("Error inesperado al ejecutar la consulta.")
#         raise


def build_view_df(dfs):
    attention = dfs["fact_attention"].alias("f")
    patient = dfs["dim_patient"].alias("p")
    speciality = dfs["dim_speciality"].alias("s")
    speciality_group = dfs["dim_speciality_group"].alias("g")
    assignment_status = dfs["dim_assignment_status"].alias("a")
    assistance_status = dfs["dim_assistance_status"].alias("ass")

    return (
        attention
        .join(patient, "PACID")
        .join(speciality, "SPEID")
        .join(speciality_group, speciality["SPEGRP_ID"] == speciality_group["SPEGRP_ID"])
        .join(assignment_status, attention["ASSIGNMENT_STATUS_ID"] == assignment_status["ASSIGNMENT_STATUS_ID"])
        .join(assistance_status, attention["ASSISTANCE_STATUS_ID"] == assistance_status["ASSISTANCE_STATUS_ID"])
        .select(
            attention["ATTID"],
            attention["ATTFIN01"].alias("FECHA_ATENCION"),
            patient["PACID"],
            patient["PACDOC"],
            patient["FIRST_NAME"],
            patient["SECOND_NAME"],
            patient["LAST_NAME"],
            patient["PACCITY"],
            patient["PACINGDAT"],
            speciality["SPENAME"],
            speciality_group["SPEGRP_NAME"],
            assignment_status["STATUS_DESC"].alias("ESTADO_ASIGNACION"),
            assistance_status["ASSISTED"].alias("ASISTENCIA")
        )
    )


def save_parquet(df, path):
    logger.info(f"Guardando vista lógica en: {path}")
    Path(path).mkdir(parents=True, exist_ok=True)
    df.repartition(4).write.mode("overwrite").parquet(path)
    logger.info("Guardado exitosamente.")


def main():
    parser = argparse.ArgumentParser(description="Construir vista lógica de atención") 
    parser.add_argument(
        "--mode", choices=["sql", "df"], default="sql",
        help="Modo de construcción: 'sql' para Spark SQL, 'df' para API de DataFrame"
    )

    args = parser.parse_args()
    
    logger.info("Iniciando proceso de generación de vista lógica de atención")

    spark = SparkSession.builder.master("local[*]").appName("erp_view_logica").getOrCreate()

    try:
        dfs = load_dataframes(
            spark,
            storage_location=STORAGE_LOCATION__SILVER_PATH,
            table_names=TABLE_NAMES
        )

        if args.mode == "sql":
            for name, df in dfs.items():
                register_temp_view(
                    df=df,
                    name=name
                )
            view_df = execute_query(
                spark, 
                path=QUERY_PATH__ATENCION_DETALLE
            )
        else:
            view_df = build_view_df(dfs=dfs)

        logger.info("Mostrando muestra de la vista generada:")
        view_df.show(truncate=False)

        save_parquet(
            df=view_df, 
            path=OUTPUT_PATH
        )
    except Exception as e:
        logger.exception("Error durante la ejecución del proceso.")
    finally:
        spark.stop()
        logger.info("Sesión Spark detenida.")


if __name__ == "__main__":
    main()