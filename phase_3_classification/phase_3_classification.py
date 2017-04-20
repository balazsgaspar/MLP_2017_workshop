
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from time import gmtime, strftime


def log(message = ""):
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " " + message)


def create_spark_ml_preprocessing_stages(label_attribute, categorical_attributes, all_cols):  
    categorical_attributes_vec = [x + "_vec" for x in categorical_attributes] 
    numerical_attributes = list(set(all_cols) - set(categorical_attributes + [label_attribute, "msisdn"]))
  
    stages = []
    for x in categorical_attributes:
        indexer = StringIndexer(inputCol=x, outputCol=x + "_index").setHandleInvalid("skip")
        encoder = OneHotEncoder(inputCol=x + "_index", outputCol=x + "_vec")
        stages.append(indexer)
        stages.append(encoder)
    
    assembler = VectorAssembler(inputCols=numerical_attributes + categorical_attributes_vec, outputCol='features')
    stages.append(assembler)
    
    
    indexer = StringIndexer(inputCol=label_attribute, outputCol="label")
    stages.append(indexer)
    
    return stages
  

def sample_data(data):
    n_churned = data.filter("churned = 'true'").count()
    n_nonchurned = data.filter("churned = 'false'").count()
    undersample_ratio = n_churned * 15.0 / n_nonchurned
    # oversample the churned (smaller) class
    churned_oversample = data.filter("churned = 'true'").sample(True, 10.0, seed=1)
    # undersample the nonchurned (larger) class
    nonchurned_undersample = data.filter("churned = 'false'").sample(True, undersample_ratio, seed=1)
    # union of both samples is the result
    final_sample = churned_oversample.unionAll(nonchurned_undersample)
    return final_sample


def run(cfg, cfg_tables, sqlContext):
    """
    Main computation block of the 2nd phase.
    :param cfg: dictionary of configuration constants.
    :param cfg_tables: dictionary of tmp table names.
    :param sqlContext: current pyspark sqlContext.
    """
    log('Running phase 3 - Classification.')
    
    log("Loading data")
    training_data = sqlContext.read.parquet(cfg_tables['TABLE_TRAIN'])
    predict_data = sqlContext.read.parquet(cfg_tables['TABLE_PREDICT'])
        
    LABEL_ATTRIBUTE = "churned"
    CATEGORICAL_ATTRIBUTES = ["rateplan_group", "rateplan_name", "committed",
                              "com_group_leader", "com_group_follower"]
    
    stages = create_spark_ml_preprocessing_stages(LABEL_ATTRIBUTE, CATEGORICAL_ATTRIBUTES, training_data.columns)
    
    rf = RandomForestClassifier(numTrees=100)
    
    pipeline = Pipeline(stages=stages + [rf])

    training_data_sampled = sample_data(training_data)
    print(training_data_sampled.groupBy("churned").count().show())
    
    
    
    model = pipeline.fit(training_data_sampled)
    predictions = model.transform(predict_data)
    
    # stats
    evaluator_roc = BinaryClassificationEvaluator(metricName="areaUnderROC")
    print('Area under ROC: {}'.format(evaluator_roc.evaluate(predictions)))
    
    
    
    return predictions
    
    
