# LeetCode: https://leetcode.com/problems/last-person-to-fit-in-the-bus/description/
# Concepts: sum over window()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark=SparkSession.builder\
    .appName("Bus Problem")\
    .master("local[*]")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

my_data=[
         (5,"Alice",250,1),
         (4,"Bob",175,5),
        (3,"Alex",350,2),
        (6,"John Cena",400,3),
        (1,"Winston",500,6),
        (2,"Marie",200,4),
         ]

my_schema=StructType(
    [
    StructField("id",IntegerType(),True),
    StructField("person_name", StringType(), True),
    StructField("weight", IntegerType(), True),
    StructField("turn", IntegerType(), True)
    ]
)
#or
columns=["id","person_name","weight","turn"]
# queue_df=spark.createDataFrame(my_data,columns)

queue_df=spark.createDataFrame(my_data,my_schema)

queue_df.show()
queue_df.printSchema()

window=Window.orderBy("turn")

cumulative_weight_df=queue_df\
    .withColumn("cumulative_weight",sum("weight").over(window))\
    .filter(col("cumulative_weight")<=1000)\
    .orderBy(col("cumulative_weight").desc())\
    .limit(1)\
    .select("person_name")

cumulative_weight_df.show()

# This query triggers two shuffles — one for window and one for global sort — which can be optimized by reusing ordering or avoiding unnecessary sorts.”
# Better to order by turn column in cumulative_weight_df query as it is already sorted on that column while computing cumulative weights using window operation


## Write your MySQL query statement below

# SQL Solution
# WITH CumalativeWeight AS (SELECT *, sum(weight) OVER (ORDER BY turn) AS total_weight FROM Queue )
# SELECT person_name FROM CumalativeWeight WHERE total_weight <=1000 ORDER BY total_weight DESC LIMIT 1



# TC: O(nlogn) as sorting required for
# ⏱️ Time Complexity (TC)
#
# Let n = number of rows in Queue
#
# 1. Window Function
# Requires sorting on turn (if not already sorted)
# Cost:
# 👉 O(n log n) (due to sorting)
# Computing cumulative sum after sorting:
# 👉 O(n)
# 2. Filtering (WHERE total_weight <= 1000)
# Linear scan
# 👉 O(n)
# 3. ORDER BY total_weight DESC + LIMIT 1
# Full sort again
# 👉 O(n log n)
#
# ⚠️ Important: Even though LIMIT 1, DB typically still sorts (unless optimized)
#
# 🚀 Final TC
# O(n log n)

# You can avoid the second sort 👇
#
# Better Approach:
#
# Use the same ordering as window function:
#
# WITH CumalativeWeight AS (
#     SELECT *,
#            sum(weight) OVER (ORDER BY turn) AS total_weight
#     FROM Queue
# )
# SELECT person_name
# FROM CumalativeWeight
# WHERE total_weight <= 1000
# ORDER BY turn DESC
# LIMIT 1;
# Why better?
# No extra sorting on total_weight
# Uses existing order (turn)
#
# 👉 Still O(n log n) overall (because of window sort),
# but avoids redundant work.
#
# ⚡ Even More Optimized (Best Possible)
#
# If DB is smart or index exists on turn:
#
# 👉 You can get close to O(n)
#
# 🧾 Final Answer (Short for Interviews)
#
# Time Complexity is O(n log n) due to sorting required for the window function and final ordering. Filtering is O(n). Optimization can remove the second sort but overall complexity remains O(n log n).


# 🧠 Space Complexity (SC)
#
# Let n = number of rows
#
# 1. Window Function (SUM() OVER)
# DB needs to maintain:
# Sorted data (on turn)
# Running cumulative sum column (total_weight)
#
# 👉 Requires:
#
# O(n)
# 2. CTE (CumalativeWeight)
# CTE is not always materialized (depends on DB)
# Some engines inline it (no extra memory)
# Some materialize → store intermediate result
#
# 👉 Worst case:
#
# O(n)
# 3. ORDER BY (Sorting)
# Sorting needs buffer space
#
# 👉 Typically:
#
# O(n)
# 🚀 Final Space Complexity
# O(n)
# ⚡ Interview-Level Insight
# ✅ Best Answer:
#
# Space complexity is O(n) due to sorting and storing intermediate results for the window function.