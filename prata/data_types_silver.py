# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import col, to_timestamp

df_consult = spark.table("`olist-prata`.geolocation")

df_consult.printSchema()


# COMMAND ----------

df_geolocation = spark.table("`olist-bronze`.geolocation")

df_geolocation_silver = (
    df_geolocation
    .withColumn(
        'geolocation_lat',
        col('geolocation_lat').cast('double')
    )    
    .withColumn(
        'geolocation_lng',
        col('geolocation_lng').cast('double')
    )
)

df_geolocation_silver.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('workspace.`olist-prata`.geolocation')


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT geolocation_city
# MAGIC FROM workspace.`olist-prata`.geolocation 
# MAGIC

# COMMAND ----------

df_order_items = spark.table("`olist-bronze`.order_items")

df_order_items_silver = (
    df_order_items
    .withColumn(
        'order_item_id',
        col('order_item_id').cast('int')
    )
    .withColumn(
        'shipping_limit_date',
        to_timestamp('shipping_limit_date')
    )
    .withColumn(
        'price',
        col('price').cast('double')
    )
    .withColumn(
        'freight_value',
        col('freight_value').cast('double')
    )
)

df_order_items_silver.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('workspace.`olist-prata`.order_items')

# COMMAND ----------

df_order_payments = spark.table("`olist-bronze`.order_payments")

df_order_payments_silver = (
    df_order_payments
    .withColumn(
        'payment_sequential',
        col('payment_sequential').cast('int')
    )
    .withColumn(
        'payment_installments',
        col('payment_installments').cast('int')
    )
    .withColumn(
        'payment_value',
        col('payment_value').cast('double')
    )
)

df_order_payments_silver.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable("workspace.`olist-prata`.order_payments")

# COMMAND ----------

df_order_reviews = spark.table("workspace.`olist-bronze`.order_reviews")

df_order_reviews_silver = (
    df_order_reviews
    .withColumn(
        'review_score',
        col('review_score').cast('int')
    )
    .withColumn(
        'review_creation_date',
        to_timestamp('review_creation_date')
    )
    .withColumn(
        'review_answer_timestamp',
        to_timestamp('review_answer_timestamp')
    )
)

df_order_reviews_silver.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable("workspace.`olist-prata`.order_reviews")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), Count(DISTINCT order_id)
# MAGIC FROM workspace.`olist-bronze`.orders

# COMMAND ----------

df_orders = spark.table("workspace.`olist-bronze`.orders")

df_orders_silver = (
    df_orders
    .withColumn(
        'order_purchase_timestamp',
        to_timestamp('order_purchase_timestamp')
    )
    .withColumn(
        'order_approved_at',
        to_timestamp('order_approved_at')
    )
    .withColumn(
        'order_delivered_carrier_date',
        to_timestamp('order_delivered_carrier_date')
    )
    .withColumn(
        'order_delivered_customer_date',
        to_timestamp('order_delivered_customer_date')
    )
    .withColumn(
        'order_estimated_delivery_date',
        to_timestamp('order_estimated_delivery_date')
    )
)

df_orders_silver.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable("workspace.`olist-prata`.orders")

# COMMAND ----------

df_products = spark.table("workspace.`olist-bronze`.products")


df_products_silver = (
    df_products
    .withColumn(
        'product_name_lenght',
        col('product_name_lenght').cast('int')
    )
    .withColumn(
        'product_description_lenght',
        col('product_description_lenght').cast('int')
    )
    .withColumn(
        'product_photos_qty',
        col('product_photos_qty').cast('int')
    )
    .withColumn(
        'product_weight_g',
        col('product_weight_g').cast('int')
    )
    .withColumn(
        'product_length_cm',
        col('product_length_cm').cast('int')
    )
    .withColumn(
        'product_height_cm',
        col('product_height_cm').cast('int')
    )
    .withColumn(
        'product_width_cm',
        col('product_width_cm').cast('int')
    )
)


df_products_silver.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable("workspace.`olist-prata`.products")
