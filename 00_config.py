
CATALOG = "sleep_mvp"          
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


RAW_VOLUME_NAME = "raw_files"
RAW_CSV_FILE_NAME = "bedtime_screentime_sleep_debt.csv"
RAW_CSV_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{RAW_VOLUME_NAME}/{RAW_CSV_FILE_NAME}"


def full_table_name(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


# COMMAND 

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{RAW_VOLUME_NAME}")

print(f"Catálogo '{CATALOG}', schemas bronze/silver/gold e o volume '{RAW_VOLUME_NAME}' estão prontos.")
print(f"Agora faça upload do CSV para: {RAW_CSV_PATH}")
