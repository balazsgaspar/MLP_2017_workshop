from pyspark import SparkContext
from pyspark.sql import HiveContext


sc = SparkContext(appName="Data exploration")
sqlContext = HiveContext(sc)


df1 = sqlContext.read.parquet("mlp_sampled_cdr_records.parquet")
df2 = sqlContext.read.parquet("mlp_sampled_ebr_base_20160401.parquet")
df3 = sqlContext.read.parquet("mlp_sampled_ebr_base_20160501.parquet")
df4 = sqlContext.read.parquet("mlp_sampled_ebr_churners_20151201_20160630.parquet")


df1.show()
df1.count()


df2.show()
df2.count()


df3.count()

df4.show()
df4.count()