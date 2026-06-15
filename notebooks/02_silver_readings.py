# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — readings
# MAGIC Clean the high-volume fact source. Principles: **quarantine, never delete**;
# MAGIC **casting is a trap for garbage**. Each input yields a clean table AND a tagged
# MAGIC quarantine table, then we reconcile against the ground-truth manifest.

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, when, lit
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# COMMAND ----------

# MAGIC %md ## Conform the drift batch and fold it in

# COMMAND ----------

drift = (spark.table("workspace.bronze.readings_drift")
    .withColumnRenamed("ts", "read_timestamp")
    .withColumnRenamed("kwh", "consumption_kwh")
    .withColumn("read_timestamp", to_timestamp("read_timestamp", "dd/MM/yyyy HH:mm"))
    .select("reading_id", "meter_id", "read_timestamp", "consumption_kwh", "read_type"))

main = (spark.table("workspace.bronze.readings")
    .withColumn("read_timestamp", to_timestamp("read_timestamp", "yyyy-MM-dd HH:mm:ss"))
    .select("reading_id", "meter_id", "read_timestamp", "consumption_kwh", "read_type"))

raw = main.unionByName(drift)
print(raw.count(), "rows after conforming + union")

# COMMAND ----------

# MAGIC %md ## Cast to real types, then deduplicate on the natural key

# COMMAND ----------

typed   = raw.withColumn("consumption_kwh", col("consumption_kwh").cast("double"))
deduped = typed.dropDuplicates(["meter_id", "read_timestamp"])
print("removed", typed.count() - deduped.count(), "duplicate readings")

# COMMAND ----------

# MAGIC %md ## Validate -> split clean vs quarantine (structural check first, then value checks)

# COMMAND ----------

valid_meters = (spark.table("workspace.bronze.meters")
                  .select("meter_id").distinct().withColumn("_ok", lit(True)))

checked = (deduped
    .join(valid_meters, "meter_id", "left")
    .withColumn("reject_reason",
        when(col("_ok").isNull(),              lit("orphan_meter"))
        .when(col("consumption_kwh").isNull(), lit("null_consumption"))
        .when(col("consumption_kwh") < 0,      lit("negative_consumption"))
        .otherwise(lit(None)))
    .drop("_ok"))

clean      = checked.filter(col("reject_reason").isNull()).drop("reject_reason")
quarantine = checked.filter(col("reject_reason").isNotNull())

# COMMAND ----------

# MAGIC %md ## Flag the keepers (estimates + implausible spikes) and write both tables

# COMMAND ----------

clean = (clean
    .withColumn("is_estimated",   col("read_type") == "E")
    .withColumn("is_implausible", col("consumption_kwh") > 5.0))

clean.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.readings")
quarantine.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.readings_quarantine")
print("clean:", spark.table("workspace.silver.readings").count(),
      "| quarantined:", spark.table("workspace.silver.readings_quarantine").count())

# COMMAND ----------

# MAGIC %md ## Reconcile against the ground-truth manifest (prove the cleaning is correct)

# COMMAND ----------

import json
defects = json.load(open("/Volumes/workspace/brightwatt/raw/_dq_manifest.json"))["defects"]
q = spark.table("workspace.silver.readings_quarantine")
def caught(r): return q.filter(col("reject_reason") == r).count()

print("orphan_meter        :", caught("orphan_meter"),         "caught vs", defects["orphan_readings"]["count"], "injected")
print("null_consumption    :", caught("null_consumption"),     "caught vs", defects["null_consumption"]["count"], "injected")
print("negative_consumption:", caught("negative_consumption"), "caught vs", defects["negative_consumption"]["count"], "injected")
# Note: duplicates removed > injected because late-arriving reads collide on the same
# (meter, timestamp) key -- a real, explainable interaction between two defect types.
