# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold — star schema (with surrogate keys + SCD2)
# MAGIC Reshape clean silver data for analysis: one central **fact** at meter x day grain,
# MAGIC surrounded by **dimensions**. Surrogate keys on every dimension; `dim_customer` is
# MAGIC SCD2 (history-preserving). Facts attach the customer_key that was current AT the event date.

# COMMAND ----------

from pyspark.sql.functions import (col, to_date, date_format, year, month, dayofmonth,
    dayofweek, lit, xxhash64, concat_ws, sum as _sum, count)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

# COMMAND ----------

# MAGIC %md ## dim_date — generated, not derived

# COMMAND ----------

dim_date = (spark.sql(
        "SELECT explode(sequence(to_date('2024-09-01'), to_date('2024-11-30'), interval 1 day)) AS date")
    .withColumn("date_key",    date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year",        year("date"))
    .withColumn("month",       month("date"))
    .withColumn("month_name",  date_format("date", "MMMM"))
    .withColumn("day",         dayofmonth("date"))
    .withColumn("day_of_week", date_format("date", "EEEE"))
    .withColumn("is_weekend",  dayofweek("date").isin(1, 7)))
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("workspace.gold.dim_date")

# COMMAND ----------

# MAGIC %md ## Dimensions with surrogate keys

# COMMAND ----------

dim_meter = (spark.table("workspace.silver.meters")
    .select("meter_id","account_id","meter_type","supply_id","install_date")
    .withColumn("meter_key", xxhash64("meter_id")))
dim_meter.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("workspace.gold.dim_meter")

dim_tariff = (spark.table("workspace.silver.tariffs").withColumn("tariff_key", xxhash64("tariff_id")))
dim_tariff.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("workspace.gold.dim_tariff")

# COMMAND ----------

# MAGIC %md ## dim_customer — SCD2 initial load (valid_from / valid_to / is_current + per-version key)

# COMMAND ----------

INIT_DATE = "2024-09-01"
dim_customer = (spark.table("workspace.silver.customers")
    .select("account_id","first_name","last_name","region","segment","tariff_id","has_email")
    .withColumn("valid_from", to_date(lit(INIT_DATE)))
    .withColumn("valid_to",   to_date(lit("9999-12-31")))
    .withColumn("is_current", lit(True))
    .withColumn("customer_key", xxhash64(concat_ws("|", col("account_id"), col("valid_from")))))
dim_customer.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("workspace.gold.dim_customer")

# COMMAND ----------

# MAGIC %md ## fact_consumption — aggregate to meter x day, attach point-in-time customer_key

# COMMAND ----------

dimc       = spark.table("workspace.gold.dim_customer").select("customer_key","account_id","valid_from","valid_to")
meter_acct = spark.table("workspace.gold.dim_meter").select("meter_id","account_id")

fact_consumption = (spark.table("workspace.silver.readings")
    .withColumn("date", to_date("read_timestamp"))
    .groupBy("meter_id", "date")
    .agg(_sum("consumption_kwh").alias("total_kwh"),
         count("*").alias("reading_count"),
         _sum(col("is_estimated").cast("int")).alias("estimated_count"),
         _sum(col("is_implausible").cast("int")).alias("implausible_count"))
    .join(meter_acct, "meter_id", "left")
    .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int")))

fact_consumption = (fact_consumption.alias("f")
    .join(dimc.alias("d"),
          (col("f.account_id") == col("d.account_id")) &
          (col("f.date").between(col("d.valid_from"), col("d.valid_to"))),
          "left")
    .select("f.*", "d.customer_key"))
fact_consumption.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("workspace.gold.fact_consumption")

# COMMAND ----------

# MAGIC %md ## fact_billing — invoice grain, shares dim_customer + dim_date

# COMMAND ----------

fact_billing = (spark.table("workspace.silver.invoices")
    .withColumn("date_key", date_format("issued_date", "yyyyMMdd").cast("int"))
    .select("invoice_id","account_id","date_key","period_start","period_end",
            "total_kwh","amount_pence","status"))
fact_billing.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("workspace.gold.fact_billing")

display(spark.sql("SHOW TABLES IN workspace.gold"))
