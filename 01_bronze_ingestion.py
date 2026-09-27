# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Ingestão do CSV bruto (Coleta — Etapa 4.2)
# MAGIC
# MAGIC Lê o CSV exatamente como foi baixado do Kaggle e grava na camada **Bronze** sem
# MAGIC nenhuma transformação de conteúdo — todas as colunas ficam como texto (string),
# MAGIC apenas com metadados de controle adicionados (`_ingested_at`, `_source_file`).
# MAGIC A tipagem correta é responsabilidade da camada Silver (próximo notebook).
# MAGIC
# MAGIC **Pré-requisito:** ter feito upload do arquivo `bedtime_screentime_sleep_debt.csv`
# MAGIC para o Volume configurado em `RAW_CSV_PATH` (veja o notebook `00_config`).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql.functions import input_file_name, lit

ingestion_ts = datetime.now(timezone.utc)

df_raw = (
    spark.read.option("header", "true")
    .option("inferSchema", "false")  # tudo como string na Bronze; tipagem é feito na Silver
    .csv(RAW_CSV_PATH)
    .withColumn("_ingested_at", lit(ingestion_ts))
    .withColumn("_source_file", input_file_name())
)

print(f"Colunas recebidas ({len(df_raw.columns)}): {df_raw.columns}")
print(f"Linhas recebidas: {df_raw.count()}")

# COMMAND ----------

(
    df_raw.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(full_table_name(BRONZE_SCHEMA, "raw_sleep_screentime"))
)

print(f"bronze.raw_sleep_screentime: {df_raw.count()} linha(s) gravada(s) em {ingestion_ts.isoformat()}.")

# COMMAND ----------

# MAGIC %md ### Amostra dos dados brutos (use um screenshot na documentação)

# COMMAND ----------

display(spark.table(full_table_name(BRONZE_SCHEMA, "raw_sleep_screentime")).limit(10))
