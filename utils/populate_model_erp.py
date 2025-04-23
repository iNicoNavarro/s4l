import os
import sys
import random
from pyspark.sql import Row
from faker import Faker
from pathlib import Path
from datetime import datetime
from .create_model_erp import get_all_schemas
from unidecode import unidecode

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import logger
from utils import spark_helpers as sh

fake = Faker('es_CO')
ZONE_MODEL: str = 'silver'
BASE_PATH: str = f"data_lake/{ZONE_MODEL}/"

def normalize(text):
    if text is None:
        return None
    return unidecode(text.strip().upper())


def set_path(name: str):
    return os.path.join(BASE_PATH, name)


def generate_dim_patient(spark, n=10):
    logger.info("Generando pacientes ficticios")
    data = []

    for i in range(1, n + 1):
        data.append(Row(
            PACID=i,
            PACDOC=str(fake.unique.random_number(digits=10, fix_len=True)),
            FIRST_NAME=normalize(fake.first_name()),
            SECOND_NAME=normalize(fake.first_name()) if random.random() < 0.5 else None,
            LAST_NAME=normalize(fake.last_name()),
            PACCITY=normalize(fake.city()),
            PACINGDAT=fake.date_time_between(start_date='-3y', end_date='now')
        ))

    df = spark.createDataFrame(data, get_all_schemas()["dim_patient"])
    sh.save_parquet(
        data=df,
        path=set_path("dim_patient")
    )
    return df


def generate_dim_speciality_group(spark):
    logger.info("Generando grupos de especialidad")
    data = [
        Row(SPEGRP_ID=1, SPEGRP_NAME="CLINICA"),
        Row(SPEGRP_ID=2, SPEGRP_NAME="QUIRURGICA"),
        Row(SPEGRP_ID=3, SPEGRP_NAME="DIAGNOSTICO Y TRATAMIENTO"),
    ]
    df = spark.createDataFrame(data, get_all_schemas()["dim_speciality_group"])
    sh.save_parquet(
        data=df,
        path=set_path("dim_speciality_group")
    )
    return df


def generate_dim_speciality(spark):
    logger.info("Generando especialidades")
    data = [
        Row(SPEID=1, SPENAME=normalize("Medicina Interna"), SPEGRP_ID=1),
        Row(SPEID=2, SPENAME=normalize("Pediatría"), SPEGRP_ID=1),
        Row(SPEID=3, SPENAME=normalize("Cirugía General"), SPEGRP_ID=2),
        Row(SPEID=4, SPENAME=normalize("Ginecología"), SPEGRP_ID=2),
        Row(SPEID=5, SPENAME=normalize("Radiología"), SPEGRP_ID=3),
    ]
    df = spark.createDataFrame(data, get_all_schemas()["dim_speciality"])
    sh.save_parquet(
        data=df,
        path=set_path("dim_speciality")
    )
    return df


def generate_dim_diagnosis(spark, n=10):
    logger.info("Generando catálogo de diagnósticos")
    cie10_catalog = [
        ("A00", "Cólera"),
        ("B01", "Varicela"),
        ("C34", "Tumor maligno de bronquios y pulmón"),
        ("E11", "Diabetes tipo 2"),
        ("F32", "Episodio depresivo"),
        ("G40", "Epilepsia"),
        ("I10", "Hipertensión esencial"),
        ("J45", "Asma"),
        ("K21", "Reflujo gastroesofágico"),
        ("M54", "Dolor lumbar")
    ]
    data = [
        Row(DIAG_ID=i+1, CIE10_CODE=code, DESCRIPTION=normalize(desc))
        for i, (code, desc) in enumerate(cie10_catalog)
    ]
    df = spark.createDataFrame(data, get_all_schemas()["dim_diagnosis"])
    sh.save_parquet(
        data=df,
        path=set_path("dim_diagnosis")
    )
    return df


def generate_dim_assignment_status(spark):
    logger.info("Generando estados de asignación")
    data = [
        Row(ASSIGNMENT_STATUS_ID=1, STATUS_DESC=normalize("Asignado")),
        Row(ASSIGNMENT_STATUS_ID=2, STATUS_DESC=normalize("Pendiente por asignar")),
        Row(ASSIGNMENT_STATUS_ID=3, STATUS_DESC=normalize("Cancelado por el paciente")),
    ]
    df = spark.createDataFrame(data, get_all_schemas()["dim_assignment_status"])
    sh.save_parquet(
        data=df,
        path=set_path("dim_assignment_status")
    )
    return df


def generate_dim_assistance_status(spark):
    logger.info("Generando estados de asistencia")
    data = [
        Row(ASSISTANCE_STATUS_ID=1, ASSISTED=normalize("Sí")),
        Row(ASSISTANCE_STATUS_ID=2, ASSISTED=normalize("No"))
    ]
    df = spark.createDataFrame(data, get_all_schemas()["dim_assistance_status"])
    sh.save_parquet(
        data=df,
        path=set_path("dim_assistance_status")
    )
    return df


def generate_fact_attention(spark, n=30):
    logger.info("Generando hechos de atención")
    schema = get_all_schemas()["fact_attention"]

    df_patients = spark.read.parquet("data_lake/silver/dim_patient")
    df_specialities = spark.read.parquet("data_lake/silver/dim_speciality")
    df_status = spark.read.parquet("data_lake/silver/dim_assignment_status")
    df_assist = spark.read.parquet("data_lake/silver/dim_assistance_status")

    pacids = [r.PACID for r in df_patients.collect()]
    speids = [r.SPEID for r in df_specialities.collect()]
    status_ids = [r.ASSIGNMENT_STATUS_ID for r in df_status.collect()]
    assist_ids = [r.ASSISTANCE_STATUS_ID for r in df_assist.collect()]

    data = []
    for i in range(1, n + 1):
        data.append(Row(
            ATTID=i,
            PACID=random.choice(pacids),
            SPEID=random.choice(speids),
            ATTFIN01=fake.date_time_between(start_date='-1y', end_date='now'),
            ASSIGNMENT_STATUS_ID=random.choice(status_ids),
            ASSISTANCE_STATUS_ID=random.choice(assist_ids)
        ))
    df = spark.createDataFrame(data, schema)
    sh.save_parquet(
        data=df,
        path=set_path("fact_attention")
    )
    return df


def generate_fact_patient_diagnosis(spark, n=30):
    logger.info("Generando hechos de diagnóstico por paciente")
    schema = get_all_schemas()["fact_patient_diagnosis"]

    df_patients = spark.read.parquet("data_lake/silver/dim_patient")
    df_diagnosis = spark.read.parquet("data_lake/silver/dim_diagnosis")

    pacids = [r.PACID for r in df_patients.collect()]
    diag_ids = [r.DIAG_ID for r in df_diagnosis.collect()]

    data = []
    for i in range(1, n + 1):
        data.append(Row(
            PATDIAG_ID=i,
            PACID=random.choice(pacids),
            DIAG_ID=random.choice(diag_ids),
            DATE=fake.date_between(start_date='-1y', end_date='today')
        ))
    df = spark.createDataFrame(data, schema)
    sh.save_parquet(
        data=df,
        path=set_path("fact_patient_diagnosis")
    )
    return df


# Function to test
def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[*]").appName("erp_populate").getOrCreate()

    Path("data_lake/silver").mkdir(parents=True, exist_ok=True)

    logger.info("Iniciando generación y escritura de dimensiones en silver")

    try:
        generators = {
            "dim_patient": generate_dim_patient(spark, n=20),
            "dim_speciality_group": generate_dim_speciality_group(spark),
            "dim_speciality": generate_dim_speciality(spark),
            "dim_diagnosis": generate_dim_diagnosis(spark),
            "dim_assignment_status": generate_dim_assignment_status(spark),
            "dim_assistance_status": generate_dim_assistance_status(spark),
        }

        for name, df in generators.items():
            logger.info(f"Dataset generado: {name}")
            df.show(truncate=False)
            df.printSchema()

            output_path = f"data_lake/silver/{name}"
            df.repartition(4).write.mode("overwrite").parquet(output_path)
            logger.info(f"Guardado en: {output_path}")

    except Exception as e:
        logger.exception("Error al generar o guardar las dimensiones.")

    finally:
        spark.stop()
        logger.info("Sesión Spark detenida.")


if __name__ == "__main__":
    main()
