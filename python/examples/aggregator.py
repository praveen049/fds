import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

class Aggregator():
    def __init__(self, aggr, st, sm, cols):
        self.columns = cols
        self.summaryt = st
        self.summarym = sm
        self.aggr = aggr
        self.conf = SparkConf()
        
    def __str__(self):
        return ('Aggregator (Method=%s, Summary=%s, SummaryMethod=%s, Columns=[%s])' %(self.aggr,self.summaryt,self.summarym,self.columns))
    
    def setconf(self, name):
        self.conf.setAppName(name)
        self.sc = SparkContext(conf=self.conf)
        self.sql = SQLContext(self.sc)

    def read_data(self):
        spark_df = self.sql.read.format("org.apache.spark.sql.cassandra").options(table="my_table", keyspace="test").load()
        return spark_df

    def get_pandas(self, spark_df):
        return spark_df.toPandas()
