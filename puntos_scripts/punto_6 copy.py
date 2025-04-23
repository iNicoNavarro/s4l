import os
import sys
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.functions import (
    col, split, element_at, create_map, lit,
    concat, to_date
)
from pyspark.sql.types import StringType
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.logger import logger

EXCEL_PATH = "data_source/fichas_diagnostico_ficticias.xlsx"
OUTPUT_PATH = "data_lake/gold/diagnosticos_transformados"
DATA_ADDRESS = "'sheet1'!A1"

MONTH_MAP = {
    "ENERO": "01", "FEBRERO": "02", "MARZO": "03", "ABRIL": "04",
    "MAYO": "05", "JUNIO": "06", "JULIO": "07", "AGOSTO": "08",
    "SEPTIEMBRE": "09", "OCTUBRE": "10", "NOVIEMBRE": "11", "DICIEMBRE": "12"
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Diagnosticos Limpios")
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5")
        .getOrCreate()
    )


def load_excel(spark, path, data_address):
    logger.info(f"Cargando archivo Excel desde: {path}")
    df = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("dataAddress", data_address)
        .load(path)
    )
    return df


def melt_dataframe(df):
    logger.info("Transformación tipo melt (stack)")
    df = df.withColumn("DOCUMENTO", col("DOCUMENTO").cast("decimal(20,0)").cast(StringType()))  # Evita notación científica
    id_col = "DOCUMENTO"
    date_cols = [c for c in df.columns if c != id_col]
    stacked_expr = f"stack({len(date_cols)}, " + ", ".join([f"'{c}', `{c}`" for c in date_cols]) + ") as (MES_ANO, DIAGNOSTICO)"
    return df.selectExpr(id_col, stacked_expr)


def clean_and_transform(df):
    month_map_expr = create_map(
        *[lit(k) for pair in MONTH_MAP.items() for k in pair]
    )
    df = df.withColumn("MESESPLIT", split(col("MES_ANO"), "\\s+")) \
           .withColumn("mes_texto", element_at(col("MESESPLIT"), 1)) \
           .withColumn("year_texto", element_at(col("MESESPLIT"), 2))
    df = df.withColumn("mes_num", month_map_expr.getItem(col("mes_texto")))
    df = df.withColumn(
        "FECHA",
        to_date(concat(lit("01/"), col("mes_num"), lit("/"), col("year_texto")),"dd/MM/yyyy")
    )
    return df.filter(col("DIAGNOSTICO").isNotNull() & (col("DIAGNOSTICO") != "")) \
             .select("DOCUMENTO", "FECHA", "DIAGNOSTICO")


def save_result(df, path):
    logger.info(f"Guardando resultados en: {path}")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(path)


def main():

    spark = create_spark_session()

    try:
        df = load_excel(spark, EXCEL_PATH, DATA_ADDRESS)

        df_melted = melt_dataframe(df)
        print("Previsualización post melt:")
        df_melted.show(truncate=False)

        df_clean = clean_and_transform(df_melted)
        print("Vista final:")
        df_clean.show(truncate=False)

        save_result(df_clean, OUTPUT_PATH)

    except Exception as e:
        logger.exception("Error al ejecutar el script del punto 6.")
    finally:
        spark.stop()
        logger.info("Sesión Spark detenida.")
        logger.info("FIN SCRIPT PUNTO 6")


if __name__ == "__main__":
    main()
