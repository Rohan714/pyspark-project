# LeetCode: https://leetcode.com/problems/find-students-who-improved/
# Concepts: window,first_value,last_value,ROWS Between

# Scores table:

# +------------+----------+-------+------------+
# | student_id | subject  | score | exam_date  |
# +------------+----------+-------+------------+
# | 101        | Math     | 70    | 2023-01-15 |
# | 101        | Math     | 85    | 2023-02-15 |
# | 101        | Physics  | 65    | 2023-01-15 |
# | 101        | Physics  | 60    | 2023-02-15 |
# | 102        | Math     | 80    | 2023-01-15 |
# | 102        | Math     | 85    | 2023-02-15 |
# | 103        | Math     | 90    | 2023-01-15 |
# | 104        | Physics  | 75    | 2023-01-15 |
# | 104        | Physics  | 85    | 2023-02-15 |
# +------------+----------+-------+------------+

# +------------+----------+-------------+--------------+
# | student_id | subject  | first_score | latest_score |
# +------------+----------+-------------+--------------+
# | 101        | Math     | 70          | 85           |
# | 102        | Math     | 80          | 85           |
# | 104        | Physics  | 75          | 85           |
# +------------+----------+-------------+--------------+


from pyspark.sql import SparkSession,Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder\
                  .master("local[*]")\
                  .appName("Students_Who_Improved.py")\
                  .getOrCreate()

my_data=(
    [
        (101,"Math",70,"2023-01-15"),
        (101, "Math", 85, "2023-02-15"),
        (101, "Physics", 65, "2023-01-15"),
        (101, "Physics", 60, "2023-02-15"),
        (102, "Math", 80, "2023-01-15"),
        (102, "Math", 85, "2023-02-15"),
        (103, "Math", 90, "2023-01-15"),
        (104, "Physics", 75, "2023-01-15"),
        (104, "Physics", 85, "2023-02-15"),
    ]
)

my_schema=StructType(
    [
        StructField("student_id",IntegerType(),True),
        StructField("subject",StringType(),True),
        StructField("score",IntegerType(),True),
        StructField("exam_date",StringType(),True),

    ]
)

df=spark.createDataFrame(data=my_data,schema=my_schema)
df=df.withColumn("exam_date",to_date(col("exam_date"),"yyyy-MM-dd"))

# df.printSchema()
# df.show()

Window_first=Window.partitionBy("student_id","subject")\
                   .orderBy(col("exam_date").asc())

Window_last=Window.partitionBy("student_id","subject")\
                  .orderBy(col("exam_date"))\
                  .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

df=df.withColumn("first_score",first(col("score")).over(Window_first))\
     .withColumn("latest_score",last(col("score")).over(Window_last))\
     .withColumn("rn",row_number().over(Window_first))\
     .filter((col("rn")==1) & (col("first_score")< col("latest_score")))\
     .selectExpr("student_id","subject","first_score","latest_score") \
    .orderBy(col("student_id"), col("subject"))

df.show()