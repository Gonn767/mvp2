# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuração do Pipeline
# MAGIC Parâmetros compartilhados por todos os notebooks do MVP (Bronze / Silver / Gold).
# MAGIC
# MAGIC **Fonte de dados:** Kaggle — ["Sleep Debt & Screen Time: Late Night Phone Habits"](https://www.kaggle.com/datasets/samartalwar/sleep-debt-and-screen-time-late-night-phone-habits)
# MAGIC — mais de 8.500 registros (um por usuário) relacionando uso de celular antes de dormir,
# MAGIC luz azul, cafeína e atividade física com débito de sono e fadiga no dia seguinte.
# MAGIC
# MAGIC **Licença:** confira o badge de licença na própria página do dataset no Kaggle
# MAGIC (aba "License") e transcreva aqui/no README antes da entrega.
# MAGIC
# MAGIC **Coleta (Etapa 4.2 — "caso simples" do enunciado):** baixe o CSV do Kaggle e faça
# MAGIC upload para um Volume do Unity Catalog. No Databricks: Catalog Explorer > seu catálogo >
# MAGIC "Create Volume" (ou use "+ New" > "Add data" > "Upload files to volume" na tela inicial).
# MAGIC Depois, ajuste `RAW_CSV_PATH` abaixo para o caminho final do arquivo.

# COMMAND ----------

CATALOG = "sleep_mvp"          # catálogo Unity Catalog do projeto
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Ajuste para o caminho onde você fez upload do CSV baixado do Kaggle
RAW_VOLUME_NAME = "raw_files"
RAW_CSV_FILE_NAME = "bedtime_screentime_sleep_debt.csv"
RAW_CSV_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{RAW_VOLUME_NAME}/{RAW_CSV_FILE_NAME}"


def full_table_name(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{RAW_VOLUME_NAME}")

print(f"Catálogo '{CATALOG}', schemas bronze/silver/gold e o volume '{RAW_VOLUME_NAME}' estão prontos.")
print(f"Agora faça upload do CSV para: {RAW_CSV_PATH}")
