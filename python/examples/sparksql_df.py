from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf()
conf.setAppName("Spark Cassandra")
#conf.setMaster("spark://192.168.112.63:7077")
#conf.set("spark.cassandra.connection.host","192.168.112.0").set("spark.cassandra.connection.port","9042")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

dataFrame = sql.read.format("org.apache.spark.sql.cassandra").options(table="my_table", keyspace="test").load()
#dataFrame.show()
dataFrame.groupBy("name").sum("value").show()
