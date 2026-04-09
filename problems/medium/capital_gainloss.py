#Leetcode:
#Learnings:
# 1.Custom sorting / ENUM : If column is ENUM('SELL','BUY'), order is defined by ENUM position, not alphabet.
# 2.Associative Property of sum, use of CASE WHEN to make price negative as per need
#There's no equivalent type in spark for enum_type in sql, just use strings
import select
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark=SparkSession.builder\
                  .master("local")\
                  .appName("capital_gainloss")\
                  .getOrCreate()
my_data=[
    ("Leetcode",'Buy',1,1000),
    ("Corona Masks", 'Buy', 2, 10),
    ("Leetcode", 'Sell', 5, 9000),
    ("Handbags", 'Buy', 17, 30000),
    ("Corona Masks", 'Sell', 3, 1010),
    ("Corona Masks", 'Buy', 4, 1000),
    ("Corona Masks", 'Sell', 5, 500),
    ("Corona Masks", 'Buy', 6, 1000),
    ("Handbags", 'Sell', 29, 7000),
    ("Corona Masks", 'Sell', 10, 10000),
]

my_schema=StructType(
    [
        StructField("stock_name",StringType(),True),
        StructField("operation",StringType(),True),
        StructField("operation_day",IntegerType(),True),
        StructField("price",IntegerType(),True),
    ]
)

df=spark.createDataFrame(data=my_data,schema=my_schema)

# df.printSchema()
# df.show()

# df_result=df.withColumn("prices",
#                          when(col("operation")=='Buy',-col("price"))
#                          .otherwise(col("price"))
#                       )
# # Always remember this order in Spark:
# # select → filter → groupBy → agg → orderBy
# # 👉 groupBy ALWAYS comes before agg
# # # select → filter → groupBy → agg → orderBy
#
# #below code will give errors: 'GroupedData' object has no attribute 'show'
# # df_result=df_result.selectExpr("stock_name","prices").agg(sum("prices")).alias("capital_gainloss")\
# #                     .groupby(col("stock_name"))
#
# df_result=df_result.groupBy("stock_name")\
#                    .agg(sum("prices")).alias("capital_gainloss")
#
# df_result.show()

df_total_price=(
                df.groupby("stock_name","operation")
                .agg(sum("price").alias("total_price"))
                )

windowSpec=Window.partitionBy("stock_name").orderBy("operation")

df_sell_buy_price=(
            df_total_price
            .withColumn("sell_price",lead(col("total_price")).over(windowSpec))
            .withColumnRenamed("total_price","buy_price")
            .filter(col("sell_price").isNotNull())
)

df_result=(
        df_sell_buy_price
        .withColumn("capital_gainloss",col("sell_price")-col("buy_price"))
        .select("stock_name","capital_gainloss")
)


df_result.show()












