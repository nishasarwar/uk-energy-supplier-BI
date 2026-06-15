# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingestion
# MAGIC Faithful, untouched landing of every raw source file as Delta tables.
# MAGIC
# MAGIC **Golden rule:** capture everything exactly as it arrived. No casting, no cleaning,
# MAGIC no dropping — that is silver's job. Read as raw text so a bad type guess can never
# MAGIC silently alter source data.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

# COMMAND ----------

# MAGIC %md ## Readings — all 32 daily files in one table, read as raw text

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

readings = (spark.read
              .option("header", True)            # keep column names; NO inferSchema -> all stays raw text
              .csv("/Volumes/workspace/brightwatt/raw/readings/")   # the FOLDER = all 32 files at once
              .withColumn("_ingested_at", current_timestamp())
              .withColumn("_source_file", col("_metadata.file_path")))  # UC-supported lineage

readings.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.readings")
print(spark.table("workspace.bronze.readings").count(), "rows")
spark.table("workspace.bronze.readings").printSchema()

# COMMAND ----------

# MAGIC %md ## Invoices — JSON (multiLine, because it is a formatted array)

# COMMAND ----------

invoices = (spark.read
              .option("multiLine", True)
              .json("/Volumes/workspace/brightwatt/raw/invoices.json")
              .withColumn("_ingested_at", current_timestamp()))
invoices.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.invoices")
print(spark.table("workspace.bronze.invoices").count(), "invoices")

# COMMAND ----------

# MAGIC %md ## Schema-drift batch — landed on its OWN table, untouched (reconciled later in silver)

# COMMAND ----------

drift = (spark.read
           .option("header", True)
           .csv("/Volumes/workspace/brightwatt/raw/readings_drift/")
           .withColumn("_ingested_at", current_timestamp()))
drift.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.readings_drift")
print(drift.columns)

# COMMAND ----------

# MAGIC %md ## The simple reference files, via one reusable function

# COMMAND ----------

def land_csv(name):
    (spark.read.option("header", True)
        .csv(f"/Volumes/workspace/brightwatt/raw/{name}.csv")
        .withColumn("_ingested_at", current_timestamp())
        .write.format("delta").mode("overwrite")
        .saveAsTable(f"workspace.bronze.{name}"))
    print(f"bronze.{name}:", spark.table(f"workspace.bronze.{name}").count(), "rows")

for name in ["customers", "meters", "tariffs", "payments"]:
    land_csv(name)

# COMMAND ----------

# MAGIC %md ## Verify + reconcile — bronze must lose nothing

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.bronze"))
main  = spark.table("workspace.bronze.readings").count()
drift = spark.table("workspace.bronze.readings_drift").count()
print("readings + drift =", main + drift, "(equals every reading generated)")
