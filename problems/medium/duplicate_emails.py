# LeetCode: https://leetcode.com/...
# Concepts: groupBy, aggregation, filtering

from utils.spark_session import get_spark
from pyspark.sql.functions import count

spark = get_spark("Duplicate Emails")

df = spark.read.csv("data/emails.csv", header=True, inferSchema=True)

result = (
    df.groupBy("email")
      .agg(count("*").alias("count"))
      .filter("count > 1")
      .select("email")
)

result.show()

spark.stop()