# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dfs = []

for tabela in spark.sql("SHOW TABLES IN `olist-prata`").collect():

    nome = tabela.tableName

    df_select = spark.table(f"`olist-prata`.{nome}").limit(1000)

    dfs.append((nome, df_select))


for nome, df_select in dfs:
    print(nome)
    display(df_select)
