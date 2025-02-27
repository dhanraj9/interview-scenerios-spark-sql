#result should have dec not oct. we need to convert it to date first
import os
import sys
from itertools import count

from pyspark.sql import SparkSession


python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\Vishwanath Dhanraj\.jdks\corretto-1.8.0_432'

spark=SparkSession.builder.getOrCreate()
data = [
    (1, 300, "31-Jan-2021"),
    (1, 400, "28-Feb-2021"),
    (1, 200, "31-Mar-2021"),
    (2, 1000, "31-Oct-2021"),
    (2, 900, "31-Dec-2021")
]
df = spark.createDataFrame(data, ["empid", "commissionamt", "monthlastdate"])
df.show()

from pyspark.sql.functions import col,max,to_date,window

df2=df.withColumn("date",to_date(col("monthlastdate"),"dd-MMM-yyyy")).drop("monthlastdate")
df2.show()

maxdate=df2.groupBy(col("empid").alias("empid1")).agg(max("date").alias("maxdate"))
maxdate.show()

join=df2.join(maxdate,(df2["empid"]==maxdate["empid1"])& (df2["date"]==maxdate["maxdate"]),"inner").drop("empid1","maxdate")
join.show()
