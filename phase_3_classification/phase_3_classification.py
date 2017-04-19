
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler



def create_spark_ml_preprocessing_stages(label_attribute, categorical_attributes):  
    # label_attribute = 'y'
    # categorical_attributes = ['c', 'd']
    categorical_attributes_vec = [x + "_vec" for x in categorical_attributes] 
    numerical_attributes = list(set(df.columns) - set(categorical_attributes + [label_attribute]))
    
    
    
    stages = []
    for x in categorical_attributes:
        indexer = StringIndexer(inputCol=x, outputCol=x + "_index")
        encoder = OneHotEncoder(inputCol=x + "_index", outputCol=x + "_vec")
        stages.append(indexer)
        stages.append(encoder)
    
    assembler = VectorAssembler(inputCols=numerical_attributes + categorical_attributes_vec, outputCol='features')
    stages.append(assembler)
    
    
    indexer = StringIndexer(inputCol=label_attribute, outputCol="label")
    stages.append(indexer)
    
    return stages
  

def run(cfg, cfg_tables, sqlContext):
    """
    Main computation block of the 2nd phase.
    :param cfg: dictionary of configuration constants.
    :param cfg_tables: dictionary of tmp table names.
    :param sqlContext: current pyspark sqlContext.
    """
    log('Running phase 3 - Classification.')
    
    # 
    
    # load data:
    log("Loading data")
    training_data = sqlContext.read.parquet(cfg_tables['TABLE_TRAIN'])
    predict_data = sqlContext.read.parquet(cfg_tables['TABLE_PREDICT'])