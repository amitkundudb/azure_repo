# Databricks notebook source
# MAGIC %run /Workspace/Users/amitkundu2022@vitbhopal.ac.in/Practice/utils

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

product_path = "/mnt/silver/sales-view/product"
store_path = '/mnt/silver/sales-view/store'

# COMMAND ----------

product_df = read_delta(product_path)
store_df = read_delta(store_path)

# COMMAND ----------

merged_product_store_df = product_df.join(store_df, product_df.store_id == store_df.store_id, "inner")
product_store_df = merged_product_store_df.select(store_df.store_id,"store_name","location","manager_name","product_id","product_name","product_code","description","category_id","price","stock_quantity","supplier_id",product_df.created_at.alias("product_created_at"),product_df.updated_at.alias("product_updated_at"),"image_url","weight","expiry_date","is_active","tax_rate")

# COMMAND ----------

customer_sales_path = "/mnt/silver/sales-view/customer_sales"
customer_sales_df = read_delta(customer_sales_path)

# COMMAND ----------

merged_prodcust_custsale_df = product_store_df.join(customer_sales_df, product_store_df.product_id == customer_sales_df.product_id, "inner")
result_df = merged_prodcust_custsale_df.select("OrderDate","Category","City","CustomerID","OrderID",product_df.product_id.alias('ProductID'),"Profit","Region","Sales","Segment","ShipDate","ShipMode","latitude","longitude","store_name","location","manager_name","product_name","price","stock_quantity","image_url")

# COMMAND ----------

path = "dbfs:/mnt/gold/sales_view/StoreProductSalesAnalysis"
save_delta(result_df,path)