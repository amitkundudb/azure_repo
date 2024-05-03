# Databricks notebook source
## Running the utils notebook to import all the fucntions inside utils

# COMMAND ----------

# MAGIC %run /Workspace/Users/amitkundu2022@vitbhopal.ac.in/Practice/utils

# COMMAND ----------

raw_df = read_csv("/mnt/bronze/sales-view/store/20240106_sales_store.csv")
# display(raw_df)

# COMMAND ----------

df = toSnakeCase(raw_df)
# display(df)

# COMMAND ----------

storecategory_df = create_domain_col(df,"store_category","email_address")
# display(storecategory_df)

# COMMAND ----------

date_format_df = date_format(storecategory_df,"created_at")
date_format_df = date_format(storecategory_df,"updated_at")
# display(date_format_df)

# COMMAND ----------

# path = "/mnt/silver/sales-view/store"
# save_delta(date_format_df,path)