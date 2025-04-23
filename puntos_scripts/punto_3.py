import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from utils.populate_model_erp import (
    generate_dim_patient,
    generate_dim_speciality_group,
    generate_dim_speciality,
    generate_dim_diagnosis,
    generate_dim_assignment_status,
    generate_dim_assistance_status,
    generate_fact_attention,
    generate_fact_patient_diagnosis
)
from utils.spark_helpers import save_parquet


def main():
    spark = SparkSession.builder.master("local[*]").appName("erp_model_orchestrator").getOrCreate()

    print("\nGenerando dimensiones...")
    df_patient = generate_dim_patient(spark, 20)
    df_speciality_group = generate_dim_speciality_group(spark)
    df_speciality = generate_dim_speciality(spark)
    df_diagnosis = generate_dim_diagnosis(spark)
    df_assignment_status = generate_dim_assignment_status(spark)
    df_assistance_status = generate_dim_assistance_status(spark)

    print("\nGenerando hechos...")
    df_fact_attention = generate_fact_attention(spark, 40)
    df_fact_patient_diagnosis = generate_fact_patient_diagnosis(spark, 30)

    print("\nMostrando resultados...")

    dfs = {
        "dim_patient": df_patient,
        "dim_speciality_group": df_speciality_group,
        "dim_speciality": df_speciality,
        "dim_diagnosis": df_diagnosis,
        "dim_assignment_status": df_assignment_status,
        "dim_assistance_status": df_assistance_status,
        "fact_attention": df_fact_attention,
        "fact_patient_diagnosis": df_fact_patient_diagnosis
    }
    
    Path("data_lake/silver").mkdir(parents=True, exist_ok=True)

    for name, df in dfs.items():
        output_path = f"data_lake/silver/{name}"
        print(f"\n=== {name} ===")
        df.show(truncate=False)
        save_parquet(
            data=df,
            path=output_path
        )
        print(f"Guardado: {output_path}")       

    spark.stop()
    print("\nFinalizado.")

if __name__ == "__main__":
    main()
