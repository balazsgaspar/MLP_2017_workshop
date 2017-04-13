from pyspark.context import SparkContext
from pyspark.sql import functions
from pyspark.sql import HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "...")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "...")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
sc._jsc.hadoopConfiguration().set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")

df1 = sqlContext.read.parquet("s3a://mlp2017/test/mlp_cdr_test.parquet")
df1.show()