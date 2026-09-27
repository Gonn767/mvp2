from pyspark.sql import functions as F

dim_user = spark.table(full_table_name(GOLD_SCHEMA, "dim_user"))
fact = spark.table(full_table_name(GOLD_SCHEMA, "fact_sleep_behavior"))

dq_results = []
total_users = dim_user.count()
total_facts = fact.count()



for col in ["age", "gender", "occupation_type", "chronotype"]:
    nulls = dim_user.filter(F.col(col).isNull()).count()
    pct = round(100 * nulls / total_users, 2) if total_users else 0
    dq_results.append(("completude", f"dim_user.{col}", f"{pct}% nulos ({nulls}/{total_users})"))

for col in ["app_key", "total_sleep_hours", "next_day_fatigue_score", "sleep_debt_category"]:
    nulls = fact.filter(F.col(col).isNull()).count()
    pct = round(100 * nulls / total_facts, 2) if total_facts else 0
    dq_results.append(("completude", f"fact_sleep_behavior.{col}", f"{pct}% nulos ({nulls}/{total_facts})"))



dup_users = total_users - dim_user.dropDuplicates(["user_id"]).count()
dq_results.append(("unicidade", "dim_user.user_id", f"{dup_users} duplicata(s)"))

dup_facts = total_facts - fact.dropDuplicates(["user_id"]).count()
dq_results.append(("unicidade", "fact_sleep_behavior.user_id", f"{dup_facts} duplicata(s)"))



valid_debt = ["Optimal Recovery", "Mild Deficit", "Moderate Debt", "Severe Sleep Debt"]
invalid_debt = [
    r["sleep_debt_category"]
    for r in fact.select("sleep_debt_category").distinct()
    .filter(F.col("sleep_debt_category").isNotNull() & ~F.col("sleep_debt_category").isin(valid_debt))
    .collect()
]
dq_results.append(("consistência", "fact_sleep_behavior.sleep_debt_category", f"valores fora do domínio: {invalid_debt}"))



age_out_of_range = dim_user.filter(F.col("age").isNotNull() & ((F.col("age") < 10) | (F.col("age") > 100))).count()
dq_results.append(("acurácia", "dim_user.age entre 10 e 100", f"{age_out_of_range} linha(s) fora da faixa"))

sleep_hours_out_of_range = fact.filter(
    F.col("total_sleep_hours").isNotNull() & ((F.col("total_sleep_hours") < 0) | (F.col("total_sleep_hours") > 24))
).count()
dq_results.append(("acurácia", "fact_sleep_behavior.total_sleep_hours entre 0 e 24", f"{sleep_hours_out_of_range} linha(s) fora da faixa"))

pct_sum_over_100 = fact.filter(
    F.col("deep_sleep_pct").isNotNull() & F.col("rem_sleep_pct").isNotNull()
    & ((F.col("deep_sleep_pct") + F.col("rem_sleep_pct")) > 100)
).count()
dq_results.append(("acurácia", "deep_sleep_pct + rem_sleep_pct <= 100", f"{pct_sum_over_100} linha(s) violam a regra"))



stats = fact.select(
    F.mean("bedtime_phone_minutes").alias("mean_phone"), F.stddev("bedtime_phone_minutes").alias("std_phone")
).collect()[0]

outlier_count = fact.filter(
    F.col("bedtime_phone_minutes").isNotNull()
    & (F.abs((F.col("bedtime_phone_minutes") - stats["mean_phone"]) / stats["std_phone"]) > 3)
).count()
dq_results.append(
    ("outliers", "fact_sleep_behavior.bedtime_phone_minutes (z-score > 3)", f"{outlier_count} linha(s) sinalizadas")
)


dq_df = spark.createDataFrame(dq_results, ["dimensao", "atributo", "resultado"])

(
    dq_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(full_table_name(GOLD_SCHEMA, "dq_report"))
)

display(dq_df)
