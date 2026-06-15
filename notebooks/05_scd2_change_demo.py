# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · SCD2 in action — processing a change
# MAGIC Demonstrates how the SCD2 dimension handles an update: a customer moves region.
# MAGIC We CLOSE the old version and INSERT a new one, then show the fact correctly
# MAGIC attributes consumption across both eras by date.
# MAGIC
# MAGIC > In production this close-and-insert is typically a single atomic Delta MERGE.
# MAGIC > The explicit form here is for clarity.

# COMMAND ----------

from pyspark.sql.functions import col, to_date, lit, date_sub, when, xxhash64, concat_ws, sum as _sum, count

CHANGE_DATE = "2024-10-15"
mover = "ACC-100016"   # example: Priya moves Wales -> London

dim = spark.table("workspace.gold.dim_customer")

# COMMAND ----------

# MAGIC %md ## 1) Close the old version  2) Insert the new version

# COMMAND ----------

closed = (dim
    .withColumn("valid_to",   when(col("account_id")==mover, date_sub(to_date(lit(CHANGE_DATE)),1)).otherwise(col("valid_to")))
    .withColumn("is_current", when(col("account_id")==mover, lit(False)).otherwise(col("is_current"))))

new_version = (dim.filter(col("account_id")==mover)
    .withColumn("region", lit("London"))
    .withColumn("valid_from", to_date(lit(CHANGE_DATE)))
    .withColumn("valid_to",   to_date(lit("9999-12-31")))
    .withColumn("is_current", lit(True))
    .withColumn("customer_key", xxhash64(concat_ws("|", col("account_id"), to_date(lit(CHANGE_DATE))))))

dim_customer = closed.unionByName(new_version)
dim_customer.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("workspace.gold.dim_customer")

dim_customer.filter(col("account_id")==mover).select(
    "customer_key","account_id","region","valid_from","valid_to","is_current").orderBy("valid_from").show(truncate=False)

# COMMAND ----------

# MAGIC %md ## Re-key the fact, then prove consumption splits across eras by date

# COMMAND ----------

# (re-run notebook 04's fact_consumption build so the temporal join picks up the new version)
# then:
display(spark.table("workspace.gold.fact_consumption").alias("f")
    .join(spark.table("workspace.gold.dim_customer").alias("d"), "customer_key")
    .filter(col("d.account_id") == mover)
    .groupBy("d.region").agg(count("*").alias("days"), _sum("total_kwh").alias("kwh")))
# Expected: ~14 days attributed to Wales (before the move), ~16 to London (after).
