# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver — reference / dimension tables
# MAGIC Calmer cleaning: cast types, standardise messy text, flag (don't quarantine)
# MAGIC valid-but-notable rows. Rule of thumb: quarantine where a row can be *invalid*;
# MAGIC flag where it is valid but notable; cast everywhere.

# COMMAND ----------

from pyspark.sql.functions import col, initcap, trim, lower, to_date

# COMMAND ----------

# MAGIC %md ## Customers — standardise regions, normalise emails, flag contactability

# COMMAND ----------

customers = (spark.table("workspace.bronze.customers")
    .withColumn("region", initcap(trim(col("region"))))   # 'NORTH WEST' / 'north west' -> 'North West'
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("has_email", col("email").isNotNull())    # GDPR/contactability flag; never fabricate
    .withColumn("signup_date", to_date("signup_date")))
customers.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.customers")
print("distinct regions:", customers.select("region").distinct().count())

# COMMAND ----------

# MAGIC %md ## Meters + tariffs — type-casting only (IDs stay strings, never numbers)

# COMMAND ----------

meters = (spark.table("workspace.bronze.meters")
    .withColumn("install_date", to_date("install_date"))
    .withColumn("base_load_kw", col("base_load_kw").cast("double")))
meters.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.meters")

tariffs = (spark.table("workspace.bronze.tariffs")
    .withColumn("unit_rate_pence", col("unit_rate_pence").cast("double"))
    .withColumn("standing_charge_pence_per_day", col("standing_charge_pence_per_day").cast("double")))
tariffs.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.tariffs")

# COMMAND ----------

# MAGIC %md ## Invoices + payments — cast dates/amounts, standardise categoricals

# COMMAND ----------

invoices = (spark.table("workspace.bronze.invoices")
    .withColumn("period_start", to_date("period_start"))
    .withColumn("period_end", to_date("period_end"))
    .withColumn("issued_date", to_date("issued_date"))
    .withColumn("total_kwh", col("total_kwh").cast("double"))
    .withColumn("amount_pence", col("amount_pence").cast("long"))
    .withColumn("status", lower(trim(col("status")))))
invoices.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.invoices")

payments = (spark.table("workspace.bronze.payments")
    .withColumn("paid_date", to_date("paid_date"))
    .withColumn("amount_pence", col("amount_pence").cast("long"))
    .withColumn("method", lower(trim(col("method")))))
payments.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.payments")

display(spark.sql("SHOW TABLES IN workspace.silver"))
