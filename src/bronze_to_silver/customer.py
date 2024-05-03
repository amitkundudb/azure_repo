# Databricks notebook source
## Running the utils notebook to import all the fucntions inside utils

# COMMAND ----------

# MAGIC %run /Workspace/Users/amitkundu2022@vitbhopal.ac.in/Practice/utils

# COMMAND ----------

raw_df = read_csv("/mnt/bronze/sales-view/customer/20240107_sales_customer.csv")
# display(raw_df)

# COMMAND ----------

df = toSnakeCase(raw_df)
# display(df)

# COMMAND ----------

spitted_df = split_name_column(df)
# display(spitted_df)

# COMMAND ----------


domain_df = create_domain_col(spitted_df,"domain","email_id")
# display(domain_df)

# COMMAND ----------


gender_df = gender_rename(domain_df)
# display(gender_df)

# COMMAND ----------


date_time_df = date_time_col(gender_df)
# display(date_time_df)

# COMMAND ----------


date_format_df = date_format(date_time_df,"join_date")
# display(date_format_df)

# COMMAND ----------


spent_df = spent_column(date_format_df)
# display(spent_df)

# COMMAND ----------


# path = "/mnt/silver/sales-view/customer"
# save_delta(spent_df,path)