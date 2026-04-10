# Leetcode: https://leetcode.com/problems/count-salary-categories/description/
# concepts: Union


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder\
                  .master("local[*]")\
                  .appName("count_salary_categories")\
                  .getOrCreate()

my_data=[
    (3,108939),
    (2, 12747),
    (8, 87709),
    (6, 91796),
]

my_schema=StructType(
    [
        StructField("account_id",StringType(),True),
        StructField("income",StringType(),True)
    ]
)

df=spark.createDataFrame(schema=my_schema,data=my_data)

# df.printSchema()
# df.show()
# select 'Low Salary' as category, count(account_id) as accounts_count
# from accounts
# where income < 20000

df_low=df.filter(col("income")<20000) \
         .agg(count(col("account_id")).alias("accounts_count"))\
         .withColumn("category",lit("Low Salary"))\
         .select("category","accounts_count")


df_high=df.filter(col("income")>50000)\
          .agg(count(col("account_id")).alias("accounts_count")) \
          .withColumn("category", lit("High Salary")) \
          .select("category","accounts_count")

df_med=df.filter((col("income")>=20000) & (col("income")<=50000))\
         .agg(count(col("account_id")).alias("accounts_count")) \
         .withColumn("category", lit("Average Salary")) \
         .select("category","accounts_count")

df_output=df_low.union(df_med).union(df_high)

df_output.show(truncate=False)


#Learnings from this
# Can agg() be used without groupBy()?
#
# 👉 Yes — and it’s very common
#
# 🔹 What it means
# df.agg(...) → aggregation over the entire DataFrame
# Equivalent to SQL:
# SELECT COUNT(*) FROM table;
# Example
# df.agg(count("account_id")).show()
#
# 👉 Output is always 1 row, even if DataFrame is empty:
#
# Non-empty → actual count
# Empty → 0
# 🔹 With groupBy
# df.groupBy("category").agg(count("*"))
#
# 👉 Now aggregation is per group, so:
#
# No rows → no groups → empty result
# 🧠 Rule to remember
# agg() → global aggregation (safe, always returns 1 row)
# groupBy().agg() → grouped aggregation (can return 0 rows)

# Also
# .fillna(0, ["accounts_count"])
# ✅ What it does
#
# It replaces NULL values with 0 in the column "accounts_count".
# Replace NULL in all columns
# df.fillna(0)

# Different values for different columns
# df.fillna({
#     "accounts_count": 0,
#     "category": "Unknown"
# })