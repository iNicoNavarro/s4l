from pyspark.sql import SparkSession
from pyspark.sql.types import *


def create_spark_session(app_name="erp_model"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()


def create_dim_patient_schema():
    return StructType([
        StructField("PACID", IntegerType(), False),
        StructField("PACDOC", StringType(), False),
        StructField("FIRST_NAME", StringType(), True),
        StructField("SECOND_NAME", StringType(), True),
        StructField("LAST_NAME", StringType(), True),
        StructField("PACCITY", StringType(), True),
        StructField("PACINGDAT", TimestampType(), True)
    ])


def create_dim_speciality_schema():
    return StructType([
        StructField("SPEID", IntegerType(), False),
        StructField("SPENAME", StringType(), False),
        StructField("SPEGRP_ID", IntegerType(), False)
    ])


def create_dim_speciality_group_schema():
    return StructType([
        StructField("SPEGRP_ID", IntegerType(), False),
        StructField("SPEGRP_NAME", StringType(), False)
    ])


def create_dim_diagnosis_schema():
    return StructType([
        StructField("DIAG_ID", IntegerType(), False),
        StructField("CIE10_CODE", StringType(), False),
        StructField("DESCRIPTION", StringType(), True)
    ])


def create_dim_assignment_status_schema():
    return StructType([
        StructField("ASSIGNMENT_STATUS_ID", IntegerType(), False),
        StructField("STATUS_DESC", StringType(), False)
    ])


def create_dim_assistance_status_schema():
    return StructType([
        StructField("ASSISTANCE_STATUS_ID", IntegerType(), False),
        StructField("ASSISTED", StringType(), False)  # SI / NO
    ])


def create_fact_attention_schema():
    return StructType([
        StructField("ATTID", IntegerType(), False),
        StructField("PACID", IntegerType(), False),
        StructField("SPEID", IntegerType(), False),
        StructField("ATTFIN01", TimestampType(), False), #TODO: Hacer nombres más sencillos de identificar (Fecha de la cita)
        StructField("ASSIGNMENT_STATUS_ID", IntegerType(), False),
        StructField("ASSISTANCE_STATUS_ID", IntegerType(), False)
    ])


def create_fact_patient_diagnosis_schema():
    return StructType([
        StructField("PATDIAG_ID", IntegerType(), False),
        StructField("PACID", IntegerType(), False),
        StructField("DIAG_ID", IntegerType(), False),
        StructField("DATE", DateType(), False)
    ])


def get_all_schemas():
    return {
        "dim_patient": create_dim_patient_schema(),
        "dim_speciality": create_dim_speciality_schema(),
        "dim_speciality_group": create_dim_speciality_group_schema(),
        "dim_diagnosis": create_dim_diagnosis_schema(),
        "dim_assignment_status": create_dim_assignment_status_schema(),
        "dim_assistance_status": create_dim_assistance_status_schema(),
        "fact_attention": create_fact_attention_schema(),
        "fact_patient_diagnosis": create_fact_patient_diagnosis_schema()
    }



def main():
    spark = create_spark_session()

    schemas = get_all_schemas()

    for name, schema in schemas.items():
        print(f"\n=== Schema: {name} ===")
        df = spark.createDataFrame([], schema)
        df.printSchema()

    spark.stop()

if __name__ == "__main__":
    main()
