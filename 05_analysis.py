# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise de Dados (Etapa 4.5, parte 2)
# MAGIC
# MAGIC Responde as perguntas de negócio definidas na Etapa de Objetivo:
# MAGIC 1. Mais tempo de celular antes de dormir está associado a maior débito de sono?
# MAGIC 2. O uso de filtro de luz azul reduz a fadiga do dia seguinte?
# MAGIC 3. O cronotipo (coruja x cotovia) influencia o total de horas dormidas e o débito de sono?
# MAGIC 4. Cafeína após as 17h está associada a maior latência para pegar no sono?
# MAGIC 5. Qual app usado antes de dormir está mais associado a débito severo de sono?
# MAGIC 6. Atividade física ao longo do dia mitiga o efeito do celular à noite sobre a fadiga?
# MAGIC 7. Certas ocupações (ex.: plantonistas de saúde) têm maior prevalência de débito severo?

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

dim_user = spark.table(full_table_name(GOLD_SCHEMA, "dim_user"))
dim_app = spark.table(full_table_name(GOLD_SCHEMA, "dim_app"))
fact = spark.table(full_table_name(GOLD_SCHEMA, "fact_sleep_behavior"))

full_view = (
    fact.alias("f")
    .join(dim_user.alias("u"), F.col("f.user_id") == F.col("u.user_id"))
    .join(dim_app.alias("a"), F.col("f.app_key") == F.col("a.app_key"), "left")
    .select(
        "u.age", "u.age_group", "u.gender", "u.occupation_type", "u.chronotype",
        "a.app_name", "a.app_category",
        "f.bedtime_phone_minutes", "f.screen_brightness_pct", "f.blue_light_filter_active",
        "f.caffeine_post_5pm_mg", "f.physical_activity_min", "f.sleep_latency_min",
        "f.total_sleep_hours", "f.deep_sleep_pct", "f.rem_sleep_pct",
        "f.morning_alarm_snoozes", "f.next_day_fatigue_score", "f.sleep_debt_category",
    )
)
full_view.createOrReplaceTempView("vw_sleep_analysis")
print(f"vw_sleep_analysis: {full_view.count()} linha(s) disponíveis para análise")

# COMMAND ----------

# MAGIC %md ## Pergunta 1 — Tempo de celular à noite x categoria de débito de sono

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sleep_debt_category,
# MAGIC        round(avg(bedtime_phone_minutes), 1) AS media_minutos_celular,
# MAGIC        count(*) AS qtd_usuarios
# MAGIC FROM vw_sleep_analysis
# MAGIC WHERE bedtime_phone_minutes IS NOT NULL AND sleep_debt_category IS NOT NULL
# MAGIC GROUP BY sleep_debt_category
# MAGIC ORDER BY media_minutos_celular DESC

# COMMAND ----------

# MAGIC %md ## Pergunta 2 — Filtro de luz azul x fadiga no dia seguinte

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT blue_light_filter_active,
# MAGIC        round(avg(next_day_fatigue_score), 2) AS fadiga_media,
# MAGIC        count(*) AS qtd_usuarios
# MAGIC FROM vw_sleep_analysis
# MAGIC WHERE next_day_fatigue_score IS NOT NULL
# MAGIC GROUP BY blue_light_filter_active

# COMMAND ----------

# MAGIC %md ## Pergunta 3 — Cronotipo x horas de sono e débito de sono

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT chronotype,
# MAGIC        round(avg(total_sleep_hours), 2) AS media_horas_sono,
# MAGIC        round(avg(next_day_fatigue_score), 2) AS fadiga_media,
# MAGIC        count(*) AS qtd_usuarios
# MAGIC FROM vw_sleep_analysis
# MAGIC WHERE chronotype IS NOT NULL
# MAGIC GROUP BY chronotype
# MAGIC ORDER BY media_horas_sono ASC

# COMMAND ----------

# MAGIC %md ## Pergunta 4 — Cafeína após as 17h x latência do sono

# COMMAND ----------

correlacao_cafeina = spark.sql(
    """
    SELECT corr(caffeine_post_5pm_mg, sleep_latency_min) AS correlacao
    FROM vw_sleep_analysis
    WHERE caffeine_post_5pm_mg IS NOT NULL AND sleep_latency_min IS NOT NULL
    """
)
display(correlacao_cafeina)

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão (edite com o resultado real do seu ambiente):** valores de correlação
# MAGIC próximos de 0 indicam associação linear fraca; próximos de 1 (ou -1) indicam associação
# MAGIC forte. Compare com a literatura: cafeína é estimulante e tende a aumentar o tempo para
# MAGIC pegar no sono, então espera-se uma correlação positiva.

# COMMAND ----------

# MAGIC %md ## Pergunta 5 — App usado antes de dormir x débito severo de sono

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT app_name, app_category,
# MAGIC        round(100 * sum(CASE WHEN sleep_debt_category = 'Severe Sleep Debt' THEN 1 ELSE 0 END) / count(*), 1) AS pct_debito_severo,
# MAGIC        count(*) AS qtd_usuarios
# MAGIC FROM vw_sleep_analysis
# MAGIC WHERE app_name IS NOT NULL AND sleep_debt_category IS NOT NULL
# MAGIC GROUP BY app_name, app_category
# MAGIC ORDER BY pct_debito_severo DESC

# COMMAND ----------

# MAGIC %md ## Pergunta 6 — Atividade física x fadiga, controlando por tempo de celular à noite

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE WHEN bedtime_phone_minutes >= 60 THEN 'Celular >= 60min antes de dormir'
# MAGIC        ELSE 'Celular < 60min antes de dormir' END AS grupo_uso_celular,
# MAGIC   CASE WHEN physical_activity_min >= 30 THEN 'Ativo (>=30min/dia)'
# MAGIC        ELSE 'Pouco ativo (<30min/dia)' END AS grupo_atividade,
# MAGIC   round(avg(next_day_fatigue_score), 2) AS fadiga_media,
# MAGIC   count(*) AS qtd_usuarios
# MAGIC FROM vw_sleep_analysis
# MAGIC WHERE bedtime_phone_minutes IS NOT NULL AND physical_activity_min IS NOT NULL AND next_day_fatigue_score IS NOT NULL
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %md ## Pergunta 7 — Ocupação x prevalência de débito severo de sono

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT occupation_type,
# MAGIC        round(100 * sum(CASE WHEN sleep_debt_category = 'Severe Sleep Debt' THEN 1 ELSE 0 END) / count(*), 1) AS pct_debito_severo,
# MAGIC        count(*) AS qtd_usuarios
# MAGIC FROM vw_sleep_analysis
# MAGIC WHERE occupation_type IS NOT NULL AND sleep_debt_category IS NOT NULL
# MAGIC GROUP BY occupation_type
# MAGIC ORDER BY pct_debito_severo DESC
