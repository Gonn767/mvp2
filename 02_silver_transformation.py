from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

bronze = spark.table(full_table_name(BRONZE_SCHEMA, "raw_sleep_screentime"))

# COMMAND 

typed = bronze.select(
    F.trim(F.col("user_id")).alias("user_id"),
    F.col("age").cast(IntegerType()).alias("age"),
    F.trim(F.col("gender")).alias("gender"),
    F.trim(F.col("occupation_type")).alias("occupation_type"),
    F.trim(F.col("chronotype")).alias("chronotype"),
    F.col("bedtime_phone_minutes").cast(IntegerType()).alias("bedtime_phone_minutes"),
    F.trim(F.col("primary_bedtime_app")).alias("primary_bedtime_app"),
    F.col("screen_brightness_pct").cast(IntegerType()).alias("screen_brightness_pct"),
    F.col("blue_light_filter_active").cast(IntegerType()).cast("boolean").alias("blue_light_filter_active"),
    F.col("caffeine_post_5pm_mg").cast(IntegerType()).alias("caffeine_post_5pm_mg"),
    F.col("physical_activity_min").cast(IntegerType()).alias("physical_activity_min"),
    F.col("sleep_latency_min").cast(DoubleType()).alias("sleep_latency_min"),
    F.col("total_sleep_hours").cast(DoubleType()).alias("total_sleep_hours"),
    F.col("deep_sleep_pct").cast(DoubleType()).alias("deep_sleep_pct"),
    F.col("rem_sleep_pct").cast(DoubleType()).alias("rem_sleep_pct"),
    F.col("morning_alarm_snoozes").cast(IntegerType()).alias("morning_alarm_snoozes"),
    F.col("next_day_fatigue_score").cast(DoubleType()).alias("next_day_fatigue_score"),
    F.trim(F.col("sleep_debt_category")).alias("sleep_debt_category"),
)

# COMMAND 

total_before = typed.count()
deduped = typed.dropDuplicates(["user_id"]).filter(F.col("user_id").isNotNull())
total_after = deduped.count()
print(f"Linhas antes da deduplicação: {total_before} | depois: {total_after} | removidas: {total_before - total_after}")

# COMMAND
VALID_GENDERS = ["Female", "Male", "Non-Binary"]
VALID_OCCUPATIONS = [
    "Healthcare / Shift Worker", "Student", "Remote Tech", "Corporate 9-to-5", "Freelance / Creative",
]
VALID_CHRONOTYPES = ["Intermediate", "Night Owl", "Morning Lark"]
VALID_APPS = [
    "Instagram / Reddit", "YouTube", "TikTok / Reels", "News / Reading",
    "Streaming (Netflix/Hulu)", "Messaging / Chat",
]
VALID_DEBT_CATEGORIES = ["Optimal Recovery", "Mild Deficit", "Moderate Debt", "Severe Sleep Debt"]

clean = (
    deduped
    .withColumn("gender", F.when(F.col("gender").isin(VALID_GENDERS), F.col("gender")).otherwise(None))
    .withColumn(
        "occupation_type",
        F.when(F.col("occupation_type").isin(VALID_OCCUPATIONS), F.col("occupation_type")).otherwise(None),
    )
    .withColumn(
        "chronotype", F.when(F.col("chronotype").isin(VALID_CHRONOTYPES), F.col("chronotype")).otherwise(None)
    )
    .withColumn(
        "primary_bedtime_app",
        F.when(F.col("primary_bedtime_app").isin(VALID_APPS), F.col("primary_bedtime_app")).otherwise(None),
    )
    .withColumn(
        "sleep_debt_category",
        F.when(F.col("sleep_debt_category").isin(VALID_DEBT_CATEGORIES), F.col("sleep_debt_category")).otherwise(
            None
        ),
    )
   
    .withColumn("age", F.when((F.col("age") >= 10) & (F.col("age") <= 100), F.col("age")).otherwise(None))
   
    .withColumn(
        "screen_brightness_pct",
        F.when((F.col("screen_brightness_pct") >= 0) & (F.col("screen_brightness_pct") <= 100),
               F.col("screen_brightness_pct")).otherwise(None),
    )
    .withColumn(
        "deep_sleep_pct",
        F.when((F.col("deep_sleep_pct") >= 0) & (F.col("deep_sleep_pct") <= 100), F.col("deep_sleep_pct")).otherwise(
            None
        ),
    )
    .withColumn(
        "rem_sleep_pct",
        F.when((F.col("rem_sleep_pct") >= 0) & (F.col("rem_sleep_pct") <= 100), F.col("rem_sleep_pct")).otherwise(
            None
        ),
    )
    
    .withColumn(
        "total_sleep_hours",
        F.when((F.col("total_sleep_hours") >= 0) & (F.col("total_sleep_hours") <= 24),
               F.col("total_sleep_hours")).otherwise(None),
    )
   
    .withColumn("bedtime_phone_minutes", F.when(F.col("bedtime_phone_minutes") >= 0, F.col("bedtime_phone_minutes")).otherwise(None))
    .withColumn("caffeine_post_5pm_mg", F.when(F.col("caffeine_post_5pm_mg") >= 0, F.col("caffeine_post_5pm_mg")).otherwise(None))
    .withColumn("physical_activity_min", F.when(F.col("physical_activity_min") >= 0, F.col("physical_activity_min")).otherwise(None))
    .withColumn("sleep_latency_min", F.when(F.col("sleep_latency_min") >= 0, F.col("sleep_latency_min")).otherwise(None))
    .withColumn("morning_alarm_snoozes", F.when(F.col("morning_alarm_snoozes") >= 0, F.col("morning_alarm_snoozes")).otherwise(None))
)


APP_CATEGORY_MAP = {
    "Instagram / Reddit": "Rede Social",
    "TikTok / Reels": "Rede Social (vídeo curto)",
    "YouTube": "Vídeo sob demanda",
    "Streaming (Netflix/Hulu)": "Streaming de entretenimento",
    "News / Reading": "Notícias / Leitura",
    "Messaging / Chat": "Mensagens",
}

app_category_expr = F.create_map([F.lit(x) for pair in APP_CATEGORY_MAP.items() for x in pair])

silver = (
    clean.withColumn(
        "age_group",
        F.when(F.col("age").isNull(), None)
        .when(F.col("age") <= 25, F.lit("18-25"))
        .when(F.col("age") <= 35, F.lit("26-35"))
        .when(F.col("age") <= 45, F.lit("36-45"))
        .when(F.col("age") <= 55, F.lit("46-55"))
        .otherwise(F.lit("56+")),
    )
    .withColumn("app_category", app_category_expr[F.col("primary_bedtime_app")])
)

(
    silver.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(full_table_name(SILVER_SCHEMA, "sleep_screentime"))
)

print(f"silver.sleep_screentime: {silver.count()} linha(s)")
display(silver.limit(10))
