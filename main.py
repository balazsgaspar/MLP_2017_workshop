from pyspark import SparkContext
from pyspark.sql import HiveContext

from util import get_cfg, add_table_suffixes
#from phase_1_data_preparation import phase_1_data_preparation
#from phase_2_data_cleaning import phase_2_data_cleaning
#from phase_3_training import phase_3_training
#from phase_4_prediction import phase_4_prediction
#from phase_5_cleanup import phase_5_cleanup



# CONFIG & LOGGER PATHS CONSTANTS
CONFIG_FILE_PATH = './config/config.cfg'
CONFIG_FILE_TMP_FILES = './config/config_tmp_files.cfg'


sc = SparkContext(appName="Churn Prediction")
sqlContext = HiveContext(sc)

cfg = get_cfg(CONFIG_FILE_PATH)
cfg
cfg_tmp_files = get_cfg(CONFIG_FILE_TMP_FILES)

cfg, cfg_tmp_files = add_table_suffixes(cfg,cfg_tmp_files)


print('Running churn prediction phases.')
phase_1_data_preparation.run(cfg, cfg_tmp_files, logger, sqlContext)
print('Phase 1 finised.')
phase_2_data_cleaning.run(cfg, cfg_tmp_files, logger, sqlContext)
print('Phase 2 finised.')
phase_3_training.run(cfg, cfg_tmp_files, logger, sqlContext)
print('Phase 3 finised.')
phase_4_prediction.run(cfg, cfg_tmp_files, logger, sqlContext)
print('Phase 4 finised.')
phase_5_cleanup.run(cfg, cfg_tmp_files, logger, sqlContext)
print('Phase 5 finised.')
print("Process done")