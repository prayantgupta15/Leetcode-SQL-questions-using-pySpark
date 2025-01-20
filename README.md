Find 100+ SQL question solutions solved using PySpark Dataframe APIs

For e.g.2346. Compute the Rank as a Percentage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, abs, round, lit,dense_rank,count
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from datetime import datetime  # Import datetime module
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("example").getOrCreate()
spark = SparkSession.builder.appName('eer').createOrReplace()
print(spark.sparkContext.getConf().getAll())


stud_schema = StructType([
    StructField('student_id',IntegerType()),
    StructField('department_id',IntegerType()),
    StructField('mark',IntegerType())
])

stud_data = [
[2          , 2             ,650],
[ 8          , 2             , 650],
[ 7          , 1             , 920],
[ 1          , 1             , 610],
[ 3          , 1             , 530]
 ]

stud_df = spark.createDataFrame(schema=stud_schema,data=stud_data)
Window.partitionBy(col('department_id'))
wd = Window.partitionBy(col('department_id')).orderBy(col('mark').desc())
cwd = Window.partitionBy(col('department_id'))
ans = stud_df.withColumn('perc',
                   (dense_rank().over(wd) -1)*100/(count('*').over(cwd)-1)
                  ).withColumn('rnk',dense_rank().over(wd))\
.withColumn('cnt',count("*").over(cwd))

ans.show()
