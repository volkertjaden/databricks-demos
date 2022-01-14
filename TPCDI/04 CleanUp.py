# Databricks notebook source
display(dbutils.fs.ls('/tmp/tpc-di'))

# COMMAND ----------

dbutils.fs.rm('/tmp/tpc-di',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database lakehouse_tpcdi_bronze cascade
