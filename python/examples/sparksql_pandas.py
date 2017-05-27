import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf()
conf.setAppName("Spark Cassandra")
#conf.setMaster("spark://192.168.112.63:7077")
#conf.set("spark.cassandra.connection.host","192.168.112.0").set("spark.cassandra.connection.port","9042")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

spark_df = sql.read.format("org.apache.spark.sql.cassandra").options(table="my_table", keyspace="test").load()
#spark_df.show()
spark_df.groupBy("name").sum("value").show()
print spark_df.select(spark_df.name)

user_pd = spark_df.toPandas()

print user_pd

