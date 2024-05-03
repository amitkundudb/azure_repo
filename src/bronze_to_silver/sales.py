# Databricks notebook source
## Running the utils notebook to import all the fucntions inside utils

# COMMAND ----------

# MAGIC %run /Workspace/Users/amitkundu2022@vitbhopal.ac.in/Practice/utils

# COMMAND ----------

raw_df = read_csv("/mnt/bronze/sales-view/sales/20240107_sales_data.csv")
# display(raw_df)

# COMMAND ----------

df = toSnakeCase(raw_df)
# display(df)

# COMMAND ----------

path = "/mnt/silver/sales-view/customer_sales"
save_delta(df,path)