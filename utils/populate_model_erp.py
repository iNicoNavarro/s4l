import random
from pyspark.sql import Row

from faker import Faker
from pathlib import Path
from datetime import datetime
from .create_model_erp import get_all_schemas
from unidecode import unidecode

fake = Faker('es_CO')


def normalize(text):
    if text is None:
        return None
    return unidecode(text.strip().upper())


def generate_dim_patient(spark, n=10):
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

    schema = get_all_schemas()["dim_patient"]
    return spark.createDataFrame(data, schema)


def generate_dim_speciality_group(spark):
    from pyspark.sql import Row
    groups = [
        Row(SPEGRP_ID=1, SPEGRP_NAME="CLINICA"),
        Row(SPEGRP_ID=2, SPEGRP_NAME="QUIRURGICA"),
        Row(SPEGRP_ID=3, SPEGRP_NAME="DIAGNOSTICO Y TRATAMIENTO"),
    ]
    schema = get_all_schemas()["dim_speciality_group"]
    return spark.createDataFrame(groups, schema)


def generate_dim_speciality(spark):
    from pyspark.sql import Row
    schema = get_all_schemas()["dim_speciality"]

    specialities = [
        Row(SPEID=1, SPENAME=normalize("Medicina Interna"), SPEGRP_ID=1),
        Row(SPEID=2, SPENAME=normalize("Pediatría"), SPEGRP_ID=1),
        Row(SPEID=3, SPENAME=normalize("Cirugía General"), SPEGRP_ID=2),
        Row(SPEID=4, SPENAME=normalize("Ginecología"), SPEGRP_ID=2),
        Row(SPEID=5, SPENAME=normalize("Radiología"), SPEGRP_ID=3),
    ]

    return spark.createDataFrame(specialities, schema)


def generate_dim_diagnosis(spark, n=10):
    from pyspark.sql import Row
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
        for i, (code, desc) in enumerate(cie10_catalog[:n])
    ]
    schema = get_all_schemas()["dim_diagnosis"]
    return spark.createDataFrame(data, schema)


def generate_dim_assignment_status(spark):
    from pyspark.sql import Row
    data = [
        Row(ASSIGNMENT_STATUS_ID=1, STATUS_DESC=normalize("Asignado")),
        Row(ASSIGNMENT_STATUS_ID=2, STATUS_DESC=normalize("Cancelado")),
        Row(ASSIGNMENT_STATUS_ID=3, STATUS_DESC=normalize("Reprogramado")),
    ]
    schema = get_all_schemas()["dim_assignment_status"]
    return spark.createDataFrame(data, schema)


def generate_dim_assistance_status(spark):
    from pyspark.sql import Row
    data = [
        Row(ASSISTANCE_STATUS_ID=1, ASSISTED=normalize("Sí")),
        Row(ASSISTANCE_STATUS_ID=2, ASSISTED=normalize("No"))
    ]
    schema = get_all_schemas()["dim_assistance_status"]
    return spark.createDataFrame(data, schema)


def generate_fact_attention(spark, n=30):
    schema = get_all_schemas()["fact_attention"]

    # Leer dimensiones necesarias
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

    return spark.createDataFrame(data, schema)


def generate_fact_patient_diagnosis(spark, n=30):
    from pyspark.sql import Row
    from etl.create_model_erp import get_all_schemas

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

    return spark.createDataFrame(data, schema)

def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[*]").appName("erp_populate").getOrCreate()

    Path("data_lake/silver").mkdir(parents=True, exist_ok=True)

    generators = {
        "dim_patient": generate_dim_patient(spark, n=20),
        "dim_speciality_group": generate_dim_speciality_group(spark),
        "dim_speciality": generate_dim_speciality(spark),
        "dim_diagnosis": generate_dim_diagnosis(spark),
        "dim_assignment_status": generate_dim_assignment_status(spark),
        "dim_assistance_status": generate_dim_assistance_status(spark),
    }

    for name, df in generators.items():
        print(f"\n=== {name} ===")
        df.show(truncate=False)
        df.printSchema()

        output_path = f"data_lake/silver/{name}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"Guardado en: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
