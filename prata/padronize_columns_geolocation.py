# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import unicodedata
from pyspark.sql.functions import udf, col, lower, trim
from pyspark.sql.types import StringType


def remover_acentos(texto):
    if texto:
        return ''.join(
            c for c in unicodedata.normalize('NFD', texto)
            if unicodedata.category(c) != 'Mn'
        )
    return texto


remove_acentos = udf(remover_acentos, StringType())

df_geo = spark.table('workspace.`olist-prata`.geolocation')

df_geo = (
    df_geo
    .withColumn(
        "geolocation_city",
        lower(trim(col("geolocation_city")))
    )
    .withColumn(
        "geolocation_city",
        remove_acentos(col("geolocation_city"))
    )
)

df_geo.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('workspace.`olist-prata`.geolocation')
