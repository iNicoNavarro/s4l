import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import logger
from utils.spark_helpers import execute_query, register_temp_view, load_parquet


QUERY_PATH: str = "puntos_scripts/queries/clave_natural.sql"
TABLE_PATH: str = "data_lake/gold/atencion_detalle"


def main():
    parser = argparse.ArgumentParser(description="Validación de clave natural en vista de atención")
    parser.add_argument(
        "--mode", choices=["sql", "df"], default="sql",
        help="Modo de validación: sql (Spark SQL) o df (DataFrame API)"
    )
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("erp_llave_natural").getOrCreate()

    try:
        df = load_parquet(spark, TABLE_PATH)

        if args.mode == "sql":
            register_temp_view(df, "atencion_detalle")
            resultado = execute_query(spark, QUERY_PATH)
        else:
            resultado = (
                df.groupBy(
                    "PACDOC", 
                    "FECHA_ATENCION"
                )
                .count()
                .filter("count > 1")
            )

        count_result = resultado.count()
        if count_result == 0:
            logger.info("La combinación propuesta es una clave natural válida (no hay duplicados).")
        else:
            logger.warning(f"Se encontraron {count_result} registros duplicados con esa combinación.")

        resultado.show(truncate=False)

    except Exception:
        logger.exception("Error durante la validación de la clave natural.")
    finally:
        spark.stop()
        logger.info("Sesión Spark finalizada.")


if __name__ == "__main__":
    main()
