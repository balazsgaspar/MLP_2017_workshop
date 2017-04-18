import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.graphx.lib._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row;

import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  

val date_from = "20160501"
val date_to = "20160503"



sc.hadoopConfiguration.set("fs.s3a.access.key", "...")
sc.hadoopConfiguration.set("fs.s3a.secret.key", "...")
sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
sc.hadoopConfiguration.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")


val df_cdr = sqlContext.read.parquet("s3a://mlp2017/data/mlp_cdr_records.parquet")
df_cdr.createOrReplaceTempView("cdr_records")


val SQL_EDGES = """
SELECT src, dst
FROM ( 
SELECT case when src > dst then src
            else dst
            end as src,
       case when src < dst then src
            else dst
            end as dst,
            date_key,
            time
  FROM (SELECT c.frommsisdn as src,
               c.tomsisdn as dst,
                             c.date_key,
                             c.time
          FROM cdr_records c
         WHERE c.year * 10000 + c.month * 100 + c.day >= """ + date_from + """
           AND c.year * 10000 + c.month * 100 + c.day <= """ + date_to + """
           AND c.frommsisdn != ''
           AND c.tomsisdn != '' 
           AND  c.frommsisdn != c.tomsisdn  
           AND (c.frommsisdn_prefix = '9900' or c.tomsisdn_prefix = '9900')
       ) tmp
GROUP BY case when src > dst then src
                             else dst
                             end,
          case when src < dst then src
               else dst
               end,
               date_key,
               time
      ) tmp2
GROUP BY src, dst
HAVING count(*) >= 10
LIMIT 100 
"""

val edges = sqlContext.sql(SQL_EDGES).cache()
val g_e = edges.map{ case Row(src: Long, dst: Long) => Edge(src, dst, 0)}  

  
// val g_edges = edges.select(col("src"), col("dst")).map { case Row(src: Long, dst: Long) => Edge(src, dst, 0) }

val default_vertex = ("Missing")
  
val graph = Graph.fromEdges(g_e.rdd,  default_vertex)

val communities = LabelPropagation.run(graph, 10)

  

  
  
  
  
