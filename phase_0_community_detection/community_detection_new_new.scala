import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.graphx.lib._
import java.util.Calendar
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)

  
  
def log(message: String) : Unit = {
    println(Calendar.getInstance.getTime + " " + message)
}
  
  
def load_edges(date_from: String, date_to: String, cdr_table: String) : org.apache.spark.sql.DataFrame = {
    val SQL_EDGES = """
        SELECT src, dst
        FROM ( 
        SELECT case when src > dst then src
                    else dst
                    end as src,
               case when src < dst then src
                    else dst
                    end as dst,
                    date_key
          FROM (SELECT c.frommsisdn as src,
                       c.tomsisdn as dst,
                                     c.date_key
                  FROM """ + cdr_table + """ c
                  WHERE date_key >= """ + date_from + """
                   AND date_key <= """ + date_to + """
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
                       date_key
              ) tmp2
        GROUP BY src, dst
        HAVING count(*) >= 10
    """
    val edges = sqlContext.sql(SQL_EDGES)
    return edges
}


// takes data frame of edges and labeld vertices (returned by the community_detection function)
// returns a new data frame of edges, where only edges connecting vertices in the same cluster are kept
def remove_intercluster_edges(edges: org.apache.spark.sql.DataFrame, labeled_vertices: org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame = {
    val cols = edges.columns
    val cols_with_label = cols :+ "label"
    val joined_edges = edges.join(labeled_vertices, col("src") === col("id"), "left_outer").select(cols_with_label.head, cols_with_label.tail: _*)
    val joined_edges2 = joined_edges.withColumnRenamed("label", "label_src").join(labeled_vertices, col("dst") === col("id"), "left_outer")
    return joined_edges2.where("label_src = label").select(cols.head, cols.tail: _*)
}



def compute_community_features(edges: org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame = {
    val g_edges = edges.map{ case Row(src: Long, dst: Long) => Edge(src, dst, 0)}.rdd

    val default_vertex = (0)
  
    val graph = Graph.fromEdges(g_edges, default_vertex)

    val communities = LabelPropagation.run(graph, 10)

    val lpa_result = communities.vertices.toDF("id", "label")

    // compute vertex degrees
    val degrees = communities.degrees.toDF("v_id", "degree_total")

    // compute vertex degrees within communities
    val inner_cluster_edges = remove_intercluster_edges(edges, lpa_result)
    val g_edges_restricted = inner_cluster_edges.map{ case Row(src: Long, dst: Long) => Edge(src, dst, 0)}.rdd  

//    val graph_restricted = Graph(g_vertices, g_edges_restricted, default_vertex)
    val graph_restricted = Graph(graph.vertices, g_edges_restricted, default_vertex)
    val community_degrees = graph_restricted.degrees.toDF("v_id_2", "degree")

    val lpa_cols = lpa_result.columns :+ "degree_total" :+ "degree"
    val lpa_result_degrees = lpa_result.join(degrees, col("id") === col("v_id"), "left_outer").join(community_degrees, col("id") === col("v_id_2"), "left_outer").select(lpa_cols.head, lpa_cols.tail: _*).na.fill(0, Seq("degree_total", "degree"))

    // compute sum of degrees for each community:
    val degree_sums = lpa_result_degrees.groupBy(col("label").alias("label_id")).agg(functions.sum(col("degree")).alias("degree_in_group"), functions.count(col("id")).alias("count_in_group"))

    val lpa_df_score = lpa_result_degrees.join(degree_sums, col("label") === col("label_id"), "left_outer").withColumn("score", functions.coalesce(col("degree") / col("degree_in_group"), functions.lit(1.0)))

    // compute min, max score for each community
    val lpa_df_score_ranges = lpa_df_score.groupBy(col("label").alias("label_id_ranges")).agg(functions.max(col("score")).alias("score_max"), functions.min(col("score")).alias("score_min"))

    // append score ranges
    val lpa_df_score_with_ranges = lpa_df_score.join(lpa_df_score_ranges, col("label") === col("label_id_ranges"), "left_outer")

    val final_columns = Seq("id", "label", "degree", "degree_total", "count_in_group", "degree_in_group", "score", "group_leader", "group_follower")
    val result = lpa_df_score_with_ranges.withColumn("group_leader", col("score") === col("score_max")).withColumn("group_follower", col("score") === col("score_min")).select(final_columns.head, final_columns.tail: _*)
    return result
}



def run(date_from: String, date_to: String, cdr_table: String, output_file: String) : Unit = {
    val edges = load_edges(date_from, date_to, cdr_table)
    val community_features = compute_community_features(edges)
    community_features.write.mode(SaveMode.Overwrite).parquet(output_file)
}



// run("20160201", "20160301", "comm_cdr_records", "lpa_result_20160201_20160301_min_cnt_10")

log("Starting community detection")
run("20160201", "20160301", "comm_cdr_records", "lpa_karel_tmp.parquet")
log("DONE")