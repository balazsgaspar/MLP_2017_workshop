from pyspark import SparkContext
from pyspark.sql import HiveContext


sc = SparkContext(appName="Community exploration")
sqlContext = HiveContext(sc)


df_lpa1 = sqlContext.read.parquet("lpa_20160301_20160401.parquet")
df_lpa2 = sqlContext.read.parquet("lpa_20160401_20160501.parquet")

df_lpa1.show()
df_lpa2.show()


degree_distribution = df_lpa1.groupBy("degree_total").count().orderBy("degree_total", ascending=False).toPandas()
degree_distribution


degrees = df_lpa1.select("degree_total").toPandas()
degrees[degrees['degree_total'] < 20000].hist(bins = 200, log=True)