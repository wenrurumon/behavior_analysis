
# coding=utf-8

import numpy as np
import pandas as pd
import time

from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils


objectValues = {
'yiwuqingjie':'bought_衣物清洁',
'yiwuhuli'   :'bought_衣物护理',
'xifa'       :'bought_洗发',
'muyu'       :'bought_沐浴',
'hufa'       :'bought_护发'
}

stringList = [
'P1M_order_num_third_cate',
'P1M_order_num_brand',
'P1M_order_num_days_active',
'P1M_view_num_third_cate',
'P1M_view_num_brand',
'P1M_view_num_days_active',
'P1M_atc_num_third_cate',
'P1M_atc_num_brand',
'P1M_atc_num_days_active',
'jd_user_level',
'gender',
'age',
'marital_status',
'education',
'profession',
'province',
'city',
'has_child',
'has_car',
'purchasing_power',
'payment_method',
'promo_sensitivity',
'browser_client']



test_dataset = get_dataset("/Unilever/Private/Acxiom/dataset/Attribute With Demo - Last 3 Month Active Customers", proxy=False)

sc = SparkContext.getOrCreate()
spark = SparkSession \
    .builder \
    .appName("demo_3") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

from pyspark.ml.feature import StringIndexer

start_time = time.time()

model = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel1509679040.38")




# transform data to df

# index string variable
test_dataset_indexed = StringIndexer(inputCol = stringList[0], outputCol = ''.join((stringList[0], '_'))).fit(test_dataset).transform(test_dataset)


for x in stringList[1:]:
    test_dataset_indexed = StringIndexer(inputCol = x, outputCol = ''.join((x, '_'))).fit(test_dataset_indexed).transform(test_dataset_indexed)

# create y at last column
from pyspark.sql.functions import udf
from pyspark.sql.types import *

udfMax = udf(lambda x1, x2: max(x1, x2), IntegerType())

test_dataset_indexed = test_dataset_indexed.withColumn('label', udfMax(test_dataset_indexed[objectValues['xifa']], test_dataset_indexed[objectValues['hufa']]))

# prune trainset
test_dataset_indexed = test_dataset_indexed.drop('user_hash')
test_dataset_indexed = test_dataset_indexed.drop('has_car_')
test_dataset_indexed = test_dataset_indexed.drop('has_child_')
test_dataset_indexed = test_dataset_indexed.drop('city_')


for k,v in objectValues.items():
    test_dataset_indexed = test_dataset_indexed.drop(v)


for x in stringList:
    test_dataset_indexed = test_dataset_indexed.drop(x)

test_dataset_indexed.show(5)

print("[Info] %s rows cost time %ss" % (test_dataset_indexed.count(), int(time.time() - start_time)))



start_time = time.time()
# Evaluate model on test instances and compute test error

test_mydf = test_dataset_indexed.rdd.map(lambda x: LabeledPoint(x[-1], x[:-1]))

test_predictions             = model.predict(test_mydf.map(lambda x: x.features))
test_labels_and_predictions  = test_mydf.map(lambda lp: lp.label).zip(test_predictions)

print("[Info] prediction cost time %ss" % ( int(time.time() - start_time)))

#my_rdd.take(5)
test_predictions.take(5)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

my_rdd = test_labels_and_predictions
newDF = sqlContext.createDataFrame(my_rdd, ['real', 'prediction'])
newDF.show(5)
type(newDF)


test = newDF.sample(False,0.001,2)
test.show(5)
start_time = time.time()
print("[Info] %s rows cost time %ss" % (test.count(), int(time.time() - start_time)))



#save_dataset('/Unilever/Private/Acxiom/dataset/askaoutput1105_2',newDF)

