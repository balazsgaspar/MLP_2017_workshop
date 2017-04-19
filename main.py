from pyspark import SparkContext
from pyspark.sql import HiveContext
from util import get_cfg, add_table_suffixes
from phase_1_data_preparation import phase_1_data_preparation
from phase_2_data_preprocessing import phase_2_data_preprocessing
from phase_3_classification import phase_3_classification


sc = SparkContext(appName="Churn Prediction")
sqlContext = HiveContext(sc)


# CONFIG & LOGGER PATHS CONSTANTS
CONFIG_FILE_PATH = './config/config.cfg'
CONFIG_FILE_TMP_FILES = './config/config_tmp_files.cfg'


sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAJW272QGBW2JQHWRA")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
# sc._jsc.hadoopConfiguration().set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")


cfg = get_cfg(CONFIG_FILE_PATH)
cfg_tables = get_cfg(CONFIG_FILE_TMP_FILES)

cfg, cfg_tables = add_table_suffixes(cfg,cfg_tables)


print('Running churn prediction phases.')
#phase_1_data_preparation.run(cfg, cfg_tables, sqlContext)
#print('Phase 1 finised.')
#phase_2_data_preprocessing.run(cfg, cfg_tables, sqlContext)
#print('Phase 2 finised.')
predictions = phase_3_classification.run(cfg, cfg_tables, sqlContext)
print('Phase 3 finised.')
#phase_4_prediction.run(cfg, cfg_tables, sqlContext)
#print('Phase 4 finised.')
#phase_5_cleanup.run(cfg, cfg_tables, sqlContext)
#print('Phase 5 finised.')
#print("Process done")



#import pandas as pd
#
#df = sqlContext.createDataFrame(pd.DataFrame({'x': [1,2], 'y': [3,4]}))
#
#df.write.parquet('karel_tmp.parquet')

