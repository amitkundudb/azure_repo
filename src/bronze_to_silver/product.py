# Databricks notebook source
## Running the utils notebook to import all the fucntions inside utils

# COMMAND ----------

# MAGIC %run /Workspace/Users/amitkundu2022@vitbhopal.ac.in/Practice/utils

# COMMAND ----------

df = read_csv("/mnt/bronze/sales-view/product/20240107_sales_product.csv")
# display(df)

# COMMAND ----------

snake_df = toSnakeCase(df)
# display(snake_df)

# COMMAND ----------


sub_df = subcategory(snake_df)
# display(sub_df)

# COMMAND ----------

# product_path = "/mnt/silver/sales-view/product"
# save_delta(sub_df,product_path)