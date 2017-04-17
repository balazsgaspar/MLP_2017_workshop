from pyspark.context import SparkContext
from pyspark.sql import functions
from pyspark.sql import HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "...")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "...")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
sc._jsc.hadoopConfiguration().set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")

df1 = sqlContext.read.parquet("s3a://mlp2017/data/mlp_cdr_records.parquet")
df2 = sqlContext.read.parquet("s3a://mlp2017/data/mlp_ebr_base_20160301.parquet")
df3 = sqlContext.read.parquet("s3a://mlp2017/data/mlp_ebr_base_20160401.parquet")
df4 = sqlContext.read.parquet("s3a://mlp2017/data/mlp_ebr_churners_20151201_20160630.parquet")
df5 = sqlContext.read.parquet("s3a://mlp2017/data/mlp_lpa_result_20160201_20160301_min_cnt_10")
df6 = sqlContext.read.parquet("s3a://mlp2017/data/mlp_lpa_result_20160301_20160401_min_cnt_10")


df1.createOrReplaceTempView("CDR")


sqlContext.setConf("spark.driver.extraClassPath", "ga-graphframes-extension-assembly-0.1.0-SNAPSHOT.jar")
sqlContext.setConf("spark.executor.extraClassPath", "ga-graphframes-extension-assembly-0.1.0-SNAPSHOT.jar")



sqlContext.sql("SELECT * FROM CDR LIMIT 4").show()



df1.show()