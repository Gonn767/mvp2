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



correlacao_cafeina = spark.sql(
    """
    SELECT corr(caffeine_post_5pm_mg, sleep_latency_min) AS correlacao
    FROM vw_sleep_analysis
    WHERE caffeine_post_5pm_mg IS NOT NULL AND sleep_latency_min IS NOT NULL
    """
)
display(correlacao_cafeina)

