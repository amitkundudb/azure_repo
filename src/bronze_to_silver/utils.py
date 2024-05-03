
# COMMAND ----------

def read_csv(path):
    raw_df = spark.read.csv(path,inferSchema=True,header = True)
    return raw_df

# COMMAND ----------

# All the column header should be in snake case in lower case (By using UDF function dynamically works for all camel case to snake case)

def toSnakeCase(raw_df):
    for column in raw_df.columns:
        snake_case_col = ''
        for char in column:
            if char == " ":
                snake_case_col = snake_case_col + "_"
            else:
                snake_case_col = snake_case_col +char.lower()
        raw_df = raw_df.withColumnRenamed(column, snake_case_col)
    return raw_df

# COMMAND ----------

# By using the "Name" column split by " " and create two columns first_name and last_name

from pyspark.sql.functions import split

def split_name_column(df):
    df = df.withColumn("first_name", split(df["name"], " ")[0]) \
             .withColumn("last_name", split(df["name"], " ")[1]).drop(df.name)
    return df

# COMMAND ----------

# # Create column domain and extract from email columns Ex: Email = "josephrice131@slingacademy.com" domain="slingacademy"

from pyspark.sql.functions import split

def create_domain_col(df, new_col, old_col):
    # Split the email address by '@' symbol and get the second part (domain)
    domain = split(df[old_col], "@").getItem(1)
    
    # Further split to exclude ".com" part and create the new column
    df = df.withColumn(new_col, split(domain, "\.com").getItem(0))
    
    return df



# COMMAND ----------

# Create a column gender where male = "M" and Female="F"
from pyspark.sql.functions import when

def gender_rename(df):
    df = df.withColumn("gender",
                   when(df["gender"] == "male", "M")
                   .otherwise("F"))
    return df

# COMMAND ----------

# From Joining date create two colums date and time by splitting based on " " delimiter.

def date_time_col(df):
    df = df.withColumn("join_date", split(df["joining_date"], " ")[0])\
        .withColumn("join_time", split(df["joining_date"], " ")[1]).drop("joining_date")
    return df

# COMMAND ----------

# Date column should be on "yyyy-MM-dd" format.

from pyspark.sql.functions import to_date

def date_format(df,col_name):
    df = df.withColumn(col_name, to_date(col_name, "MM-dd-yyyy"))
    return df

# COMMAND ----------

# Create a column expenditure-status, based on spent column is 
# spent below 200 column value is "MINIMUM" else "MAXIMUM"
def spent_column(df):
    df = df.withColumn("spent_status",when(df["spent"]<200,"MINIMUM").otherwise("MAXIMUM"))
    return df

# COMMAND ----------

# Write based on upsert [table_name: customer] 
# (in silver layer path is silver/sales_view/tablename/{delta pearquet}

def save_delta(df,path):
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)

# COMMAND ----------

# •	Create a column sub_category (Use Category columns id category_id=1, "phone"; 2, "laptop"; 3,"playstation"; 4,"e-device"

def subcategory(df):
    df = df.withColumn("sub_category",
                       when(df["category_id"] == 1, "phone")
                       .when(df["category_id"] == 2, "laptop")
                       .when(df["category_id"] == 3, "playstation")
                       .when(df["category_id"] == 4, "e-device")
                       .otherwise("other"))
    return df


# COMMAND ----------

def read_delta(path):
    df = spark.read.format("delta").load(path)
    return df