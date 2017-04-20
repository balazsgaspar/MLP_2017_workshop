from pyspark import SparkContext
from pyspark.sql import HiveContext
from util import get_cfg, add_table_suffixes
from phase_1_data_preparation import phase_1_data_preparation
from phase_2_data_preprocessing import phase_2_data_preprocessing
from phase_3_classification import phase_3_classification


from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf

sc = SparkContext(appName="Churn Prediction")
sqlContext = HiveContext(sc)


# CONFIG & LOGGER PATHS CONSTANTS
CONFIG_FILE_PATH = './config/config.cfg'
CONFIG_FILE_TMP_FILES = './config/config_tmp_files.cfg'


cfg = get_cfg(CONFIG_FILE_PATH)
cfg_tables = get_cfg(CONFIG_FILE_TMP_FILES)

cfg, cfg_tables = add_table_suffixes(cfg,cfg_tables)


print('Running churn prediction phases.')
phase_1_data_preparation.run(cfg, cfg_tables, sqlContext)
print('Phase 1 finised.')
phase_2_data_preprocessing.run(cfg, cfg_tables, sqlContext)
print('Phase 2 finised.')
predictions = phase_3_classification.run(cfg, cfg_tables, sqlContext)
print('Phase 3 finised.')


split1_udf = udf(lambda value: value[0].item(), FloatType())
split2_udf = udf(lambda value: value[1].item(), FloatType())

predictions = predictions.withColumn("probability_nonchurned", split1_udf('probability').alias('c1'))
predictions = predictions.withColumn("probability_churned", split2_udf('probability').alias('c2'))
#output2 = randomforestoutput.select(split1_udf('probability').alias('c1'), split2_udf('probability').alias('c2'))

predictions.groupBy("label").count().show()

N = 1000
#predictions.filter("prediction = 1.0").orderBy("probability_churned", ascending = False)\
#   .select("label", "churned", "probability_nonchurned", "probability_churned", "probability").show()

top_potential_churners = predictions.filter("prediction = 1.0").orderBy("probability_churned", ascending = False).limit(N)
predictions_counts = top_potential_churners.groupBy("label").count()
predictions_counts = predictions_counts.withColumn("fraction", predictions_counts["count"] * 1.0 / N)
predictions_counts.show()

stats = predictions.groupBy("label", "prediction").count().toPandas()