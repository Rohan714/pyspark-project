#Leetcode: https://leetcode.com/problems/odd-and-even-transactions/

# +----------------+--------+------------------+
# | transaction_id | amount | transaction_date |
# +----------------+--------+------------------+
# | 1              | 150    | 2024-07-01       |
# | 2              | 200    | 2024-07-01       |
# | 3              | 75     | 2024-07-01       |
# | 4              | 300    | 2024-07-02       |
# | 5              | 50     | 2024-07-02       |
# | 6              | 120    | 2024-07-03       |
# +----------------+--------+------------------+

# output
# +------------------+---------+----------+
# | transaction_date | odd_sum | even_sum |
# +------------------+---------+----------+
# | 2024-07-01       | 75      | 350      |
# | 2024-07-02       | 0       | 350      |
# | 2024-07-03       | 0       | 120      |
# +------------------+---------+----------+

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder\
                  .master("local[*]")\
                  .appName("Odd Even Transaction")\
                  .getOrCreate()

my_data=[
    (1,150,"2024-07-01"),
    (2,200,"2024-07-01"),
    (3,75,"2024-07-01"),
    (4,300,"2024-07-02"),
    (5,50,"2024-07-02"),
    (6,120,"2024-07-03"),
]

my_schema = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("amount",IntegerType(), True),
        StructField("transaction_date",StringType(), True),
    ]
)

df=spark.createDataFrame(data=my_data,schema=my_schema)
df=df.withColumn("transaction_date",to_date(col("transaction_date")))

# df.printSchema()
# df.show()

df=df.withColumn("even_sum",when(col("amount")%2==0,col("amount")).otherwise(0))\
     .withColumn("odd_sum",when(col("amount")%2!=0,col("amount")).otherwise(0))\
     .groupBy(col("transaction_date"))\
     .agg(sum(col("odd_sum")).alias("odd_sum"),sum(col("even_sum")).alias("even_sum"))

df.show()