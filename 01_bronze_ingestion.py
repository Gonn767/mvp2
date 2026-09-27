from datetime import datetime, timezone

from pyspark.sql.functions import input_file_name, lit

ingestion_ts = datetime.now(timezone.utc)

df_raw = (
    spark.read.option("header", "true")
    .option("inferSchema", "false")  
    .csv(RAW_CSV_PATH)
    .withColumn("_ingested_at", lit(ingestion_ts))
    .withColumn("_source_file", input_file_name())
)

print(f"Colunas recebidas ({len(df_raw.columns)}): {df_raw.columns}")
print(f"Linhas recebidas: {df_raw.count()}")

# COMMAND

(
    df_raw.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(full_table_name(BRONZE_SCHEMA, "raw_sleep_screentime"))
)

print(f"bronze.raw_sleep_screentime: {df_raw.count()} linha(s) gravada(s) em {ingestion_ts.isoformat()}.")

# COMMAND



# COMMAND

display(spark.table(full_table_name(BRONZE_SCHEMA, "raw_sleep_screentime")).limit(10))
