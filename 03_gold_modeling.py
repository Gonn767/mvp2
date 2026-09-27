# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: Modelagem dimensional / Esquema Estrela (Etapas 4.3 e 4.4)
# MAGIC
# MAGIC **Fato central:** comportamento noturno e métricas de sono por usuário
# MAGIC (`fact_sleep_behavior`) — granularidade: 1 linha por usuário (o dataset é uma
# MAGIC foto única por pessoa, sem série temporal).
# MAGIC
# MAGIC **Dimensões:** perfil demográfico do usuário (`dim_user`), app usado antes de dormir
# MAGIC (`dim_app`).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table(full_table_name(SILVER_SCHEMA, "sleep_screentime"))

# COMMAND ----------

# MAGIC %md ### `dim_user`

# COMMAND ----------

dim_user = silver.select(
    "user_id",
    "age",
    "age_group",
    "gender",
    "occupation_type",
    "chronotype",
).dropDuplicates(["user_id"])

(
    dim_user.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(full_table_name(GOLD_SCHEMA, "dim_user"))
)
print(f"gold.dim_user: {dim_user.count()} linha(s)")

# COMMAND ----------

# MAGIC %md ### `dim_app` — dimensão pequena, com chave substituta (surrogate key)

# COMMAND ----------

dim_app = (
    silver.select("primary_bedtime_app", "app_category")
    .filter(F.col("primary_bedtime_app").isNotNull())
    .dropDuplicates(["primary_bedtime_app"])
    .withColumn("app_key", F.row_number().over(Window.orderBy("primary_bedtime_app")))
    .select("app_key", F.col("primary_bedtime_app").alias("app_name"), "app_category")
)

(
    dim_app.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(full_table_name(GOLD_SCHEMA, "dim_app"))
)
print(f"gold.dim_app: {dim_app.count()} linha(s)")
display(dim_app)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `fact_sleep_behavior`
# MAGIC Granularidade: 1 linha por usuário. Referencia `dim_user` por `user_id` e `dim_app`
# MAGIC por `app_key`.

# COMMAND ----------

fact_sleep_behavior = (
    silver.alias("s")
    .join(dim_app.alias("a"), F.col("s.primary_bedtime_app") == F.col("a.app_name"), "left")
    .select(
        F.col("s.user_id"),
        F.col("a.app_key"),
        "bedtime_phone_minutes",
        "screen_brightness_pct",
        "blue_light_filter_active",
        "caffeine_post_5pm_mg",
        "physical_activity_min",
        "sleep_latency_min",
        "total_sleep_hours",
        "deep_sleep_pct",
        "rem_sleep_pct",
        "morning_alarm_snoozes",
        "next_day_fatigue_score",
        "sleep_debt_category",
    )
)

(
    fact_sleep_behavior.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(full_table_name(GOLD_SCHEMA, "fact_sleep_behavior"))
)
print(f"gold.fact_sleep_behavior: {fact_sleep_behavior.count()} linha(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catálogo de Dados (auxiliar)
# MAGIC Rode a célula abaixo e tire um screenshot para compor o Catálogo de Dados da Etapa 4.3.

# COMMAND ----------

for schema, table in [
    (GOLD_SCHEMA, "dim_user"),
    (GOLD_SCHEMA, "dim_app"),
    (GOLD_SCHEMA, "fact_sleep_behavior"),
]:
    print(f"\n=== {CATALOG}.{schema}.{table} ===")
    spark.sql(f"DESCRIBE TABLE {full_table_name(schema, table)}").show(50, truncate=False)
