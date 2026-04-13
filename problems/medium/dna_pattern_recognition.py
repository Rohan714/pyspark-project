# Leetcode: https://leetcode.com/problems/dna-pattern-recognition/description/
#concept: LIKE and regex

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder\
                  .master("local[*]")\
                  .appName("DNA Pattern Recognition")\
                  .getOrCreate()

my_data = [
    (1, "ATGCTAGCTAGCTAA", "Human"),
    (2, "GGGTCAATCATC", "Human"),
    (3, "ATATATCGTAGCTA", "Human"),
    (4, "ATGGGGTCATCATAA", "Mouse"),
    (5, "TCAGTCAGTCAG", "Mouse"),
    (6, "ATATCGCGCTAG", "Zebrafish"),
    (7, "CGTATGCGTCGTA", "Zebrafish")
]

my_schema=StructType(
    [
        StructField("sample_id",IntegerType(),True),
        StructField("dna_sequences",StringType(),True),
        StructField("species",StringType(),True)
    ]
)

df=spark.createDataFrame(data=my_data,schema=my_schema)

# df.printSchema()
# df.show()


result_df=df.withColumn("has_start",when(col("dna_sequences").like("ATG%"),1).otherwise(0))\
            .withColumn("has_stop",when((col("dna_sequences").like("%TAA")) | (col("dna_sequences").like("%TAG")) | (col("dna_sequences").like("%TGA")),1).otherwise(0))\
            .withColumn("has_atat",when(col("dna_sequences").like("%ATAT%"),1).otherwise(0))\
            .withColumn("has_ggg",when(col("dna_sequences").like("%GGG%"),1).otherwise(0))

result_df.show()