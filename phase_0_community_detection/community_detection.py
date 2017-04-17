from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)


import pandas as pd

vertices = sqlContext.createDataFrame(pd.DataFrame({'id': [1,2,3]}))
edges = sqlContext.createDataFrame(pd.DataFrame({'src': [1,2,3], 'dst': [2,3,1], 'weight': [1,2,1]}))

weight_column = "weight"
max_iter = 10



sqlContext.setConf("spark.driver.extraClassPath", "/home/cdsw/phase_0_community_detection/ga-graphframes-extension-assembly-0.1.0-SNAPSHOT.jar")
sqlContext.setConf("spark.executor.extraClassPath", "/home/cdsw/phase_0_community_detection/ga-graphframes-extension-assembly-0.1.0-SNAPSHOT.jar")

sc._jsc.sc().addJar("/home/cdsw/phase_0_community_detection/ga-graphframes-extension-assembly-0.1.0-SNAPSHOT.jar")

sc.addPyFile("phase_0_community_detection/graphframe.py")




from graphframe import *


g = GraphFrame(vertices, edges)

lpa = g.weightedLabelPropagation(weights=weight_column, maxIter=max_iter)

