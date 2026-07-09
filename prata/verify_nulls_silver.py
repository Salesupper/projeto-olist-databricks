# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import col, sum


tabelas = spark.sql("SHOW TABLES IN `olist-bronze`").collect()


for tabela in tabelas:

    nome = tabela.tableName
    
    df = spark.table(f"`olist-bronze`.{nome}")

    print(f"===== {nome} =====")

    df_nulls = df.select(
        [
            sum(col(c).isNull().cast("int")).alias(c)
            for c in df.columns
        ]
    )

    display(df_nulls)
