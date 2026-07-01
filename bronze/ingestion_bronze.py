# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
landing_path = "/Volumes/workspace/olist-landing-zone/landing-zone"

# COMMAND ----------

spark.conf.set(
    "spark.sql.session.timeZone",
    "America/Sao_Paulo"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_timestamp AS ts_sao_paulo

# COMMAND ----------

import os 

arquivos = os.listdir(landing_path)

arquivos = [x for x in arquivos]

arquivos

# COMMAND ----------

nome_tabelas = {
    'product_category_name_translation.csv':    'product_category_name_translation'
    ,'olist_customers_dataset.csv':             'customers'
    ,'olist_geolocation_dataset.csv':           'geolocation'
    ,'olist_order_items_dataset.csv':           'order_items'
    ,'olist_order_payments_dataset.csv':        'order_payments'
    ,'olist_order_reviews_dataset.csv':         'order_reviews'
    ,'olist_orders_dataset.csv':                'orders'
    ,'olist_products_dataset.csv':              'products'
    ,'olist_sellers_dataset.csv':               'sellers'
}

nome_tabelas

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

for arquivo, tabela in nome_tabelas.items():

    df_land = spark.read.csv(
        f"{landing_path}/{arquivo}",
        header=True,
        inferSchema=False,
        sep=",",
        quote='"',
        escape='"',
        multiLine=True
    )

    df_bronze = df_land.withColumn("ts_proc", current_timestamp())

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"workspace.`olist-bronze`.{tabela}")
