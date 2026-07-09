# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
df_orders    = spark.table("workspace.`olist-prata`.orders")
df_items     = spark.table("workspace.`olist-prata`.order_items").drop("ts_proc")
df_products  = spark.table("workspace.`olist-prata`.products").drop("ts_proc")
df_customers = spark.table("workspace.`olist-prata`.customers").drop("ts_proc")
df_sellers   = spark.table("workspace.`olist-prata`.sellers").drop("ts_proc")
df_category  = spark.table("workspace.`olist-prata`.product_category_name_translation").drop("ts_proc")

# COMMAND ----------

# MAGIC %md
# MAGIC #Tabela Fato Pagamentos

# COMMAND ----------

df_orders = spark.table("workspace.`olist-prata`.orders")
df_customers = spark.table("workspace.`olist-prata`.customers").drop("ts_proc")
df_payments = spark.table("workspace.`olist-prata`.order_payments").drop("ts_proc")


df_fact_payments = (
    df_orders
    
    .join(
        df_customers,
        "customer_id",
        "left"
    )

    .join(
        df_payments,
        "order_id",
        "left"
    )
)

display(df_fact_payments.limit(100))

df_fact_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.`olist-ouro`.fact_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC #Tabela Fato Avaliações

# COMMAND ----------

df_orders = spark.table("workspace.`olist-prata`.orders")
df_customers = spark.table("workspace.`olist-prata`.customers").drop("ts_proc")
df_reviews = spark.table("workspace.`olist-prata`.order_reviews").drop("ts_proc")

df_fact_reviews = (
    df_orders

    .join(
        df_customers,
        "customer_id",
        "left"
    )

    .join(
        df_reviews,
        "order_id",
        "left"
    )
)

display(df_fact_reviews.limit(100))

df_fact_reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.`olist-ouro`.fact_reviews")


# COMMAND ----------

# MAGIC %md
# MAGIC #Tabela Dimensão Geolocation

# COMMAND ----------

from pyspark.sql.functions import avg, first

df_geo = spark.table("workspace.`olist-prata`.geolocation")

df_dim_geolocation = (
    df_geo
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        avg("geolocation_lat").alias("latitude"),
        avg("geolocation_lng").alias("longitude"),
        first("geolocation_city").alias("city"),
        first("geolocation_state").alias("state")
    )
)

display(df_dim_geolocation.limit(100))

df_dim_geolocation.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.`olist-ouro`.dim_geolocation")

# COMMAND ----------

# MAGIC %md
# MAGIC #Tabela Fato Vendas

# COMMAND ----------

df_fact_orders = (
    df_orders

    .join(df_customers, "customer_id", "left")
    .join(
        df_dim_geolocation,
        df_customers.customer_zip_code_prefix == df_dim_geolocation.geolocation_zip_code_prefix,
        "left"
    )
    .join(df_items, "order_id", "left")
    .join(df_products, "product_id", "left")
    .join(df_category, "product_category_name", "left")
    .join(df_sellers, "seller_id", "left")
)

display(df_fact_orders.limit(100))

df_fact_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.`olist-ouro`.fact_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE workspace.`olist-ouro`.fact_orders
