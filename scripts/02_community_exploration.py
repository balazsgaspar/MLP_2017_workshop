from pyspark import SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F

sc = SparkContext(appName="Community exploration")
sqlContext = HiveContext(sc)


df_lpa1 = sqlContext.read.parquet("lpa_20160301_20160401.parquet")
df_lpa2 = sqlContext.read.parquet("lpa_20160401_20160501.parquet")

df_lpa1.show()
df_lpa2.show()

# num of vertices
df_lpa1.count()
sum_degrees = df_lpa1.select(F.sum(F.col("degree_total"))).first()[0]
sum_degrees
# num of edges
n_edges = sum_degrees / 2
n_edges

# degree distribution
degree_distribution = df_lpa1.groupBy("degree_total").count().orderBy("degree_total", ascending=False).toPandas()
degree_distribution

degrees = df_lpa1.select("degree_total").toPandas()
degrees[degrees['degree_total'] < 20000].hist(bins = 200, log=True)

# community sizes
com_sizes = df_lpa1.groupBy("label").count().withColumnRenamed("count", "community_size")
com_sizes_cnts = com_sizes.groupBy("community_size").count()
com_sizes_cnts.show()

df = com_sizes_cnts.toPandas()
df.sort("community_size", inplace=True)
df.plot.bar(x = "community_size", y = "count")