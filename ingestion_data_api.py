# Databricks notebook source
# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %run ./env

# COMMAND ----------

from kaggle.api.kaggle_api_extended import KaggleApi

try:
    api = KaggleApi()
    api.authenticate()

    pasta = '/Volumes/workspace/olist-landing-zone/landing-zone/'

    api.dataset_download_files(
        "olistbr/brazilian-ecommerce",
        path = pasta,
        unzip = True
    )
    print('dataset instalado com sucesso')

except Exception as e:
    print(f'ocorreu um erro ao baixar o dataset, Erro: {e}')


# COMMAND ----------

landing_path = "/Volumes/workspace/olist-landing-zone/landing-zone"

# COMMAND ----------

arq = "olist_products_dataset.csv"

df_product = spark.read.csv(
    f"{landing_path}/{arq}",
    header=True,
    inferSchema=False,
    sep = ","
)

display(df_product.limit(100))
