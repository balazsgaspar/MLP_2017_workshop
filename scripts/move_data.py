from pyspark import SparkContext
from pyspark.sql import HiveContext



sc = SparkContext(appName="Move data")
sqlContext = HiveContext(sc)


sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "...")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "...")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")


df1 = sqlContext.read.parquet("s3a://mlp2017/pub/mlp_sampled_cdr_records.parquet")
df2 = sqlContext.read.parquet("s3a://mlp2017/pub/mlp_sampled_ebr_base_20160401.parquet")
df3 = sqlContext.read.parquet("s3a://mlp2017/pub/mlp_sampled_ebr_base_20160501.parquet")
df4 = sqlContext.read.parquet("s3a://mlp2017/pub/mlp_sampled_ebr_churners_20151201_20160630.parquet")


df1.write.parquet("mlp_sampled_cdr_records.parquet", mode="overwrite")
df2.write.parquet("mlp_sampled_ebr_base_20160401.parquet", mode="overwrite")
df3.write.parquet("mlp_sampled_ebr_base_20160501.parquet", mode="overwrite")
df4.write.parquet("mlp_sampled_ebr_churners_20151201_20160630.parquet", mode="overwrite")
