
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
# Evaluate model on tesWt instances and compute test error

test_mydf = test_dataset_indexed.rdd.map(lambda x: LabeledPoint(x[-1], x[:-1]))

test_predictions             = model.predict(test_mydf.map(lambda x: x.features))
test_labels_and_predictions  = test_mydf.map(lambda lp: lp.label).zip(test_predictions)

print("[Info] prediction cost time %ss" % ( int(time.time() - start_time)))

test_labels_and_predictions.take(5)

###################################



start_time = time.time()

bins = {}
bins['0'] = [0.0, 0.1, 0]
bins['1'] = [0.1, 0.2, 0]
bins['2'] = [0.2, 0.3, 0]
bins['3'] = [0.3, 0.4, 0]
bins['4'] = [0.4, 0.5, 0]
bins['5'] = [0.5, 0.6, 0]
bins['6'] = [0.6, 0.7, 0]
bins['7'] = [0.7, 0.8, 0]
bins['8'] = [0.8, 0.9, 0]
bins['9'] = [0.9, 1.0, 0]

test_labels_and_predictions.cache()
test_labels_and_predictions.take(1)

for k,v in bins.items():
    v[2] = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()


import pprint
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(bins)

print("[Info] cost time %ss" % int(time.time() - start_time)) 

'''
#zhou
k = '0'
v = bins[k]
v[2] = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
v

#hu
v = bins['0'];v
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count();k
bins['0'][2] = k
v = bins['1']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['1'][2] = k
v = bins['2']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['2'][2] = k
v = bins['3']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['3'][2] = k
v = bins['4']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['4'][2] = k
v = bins['5']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['5'][2] = k
v = bins['6']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['6'][2] = k
v = bins['7']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['7'][2] = k
v = bins['8']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['8'][2] = k
v = bins['9']
k = test_labels_and_predictions.filter(lambda lp: lp[1] >= v[0] and lp[1] < v[1]).count()
bins['9'][2] = k
'''


###################################


tb1 = dataset["user_hash", 'bought_衣物清洁']
tb2 = dataset['P1M_order_CPG', 'city']
from pyspark.sql import functions


# This will return a new DF with all the columns + id
tb1 = tb1.withColumn("id", functions.monotonically_increasing_id())
tb2 = tb2.withColumn("id", functions.monotonically_increasing_id())


# Perform a join on the ids.
df3 = tb2.join(tb1, "id", "outer").drop("id")
df3.show()





####################################


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
my_rdd = test_labels_and_predictions
newDF = sqlContext.createDataFrame(my_rdd, ['real', 'prediction'])
newDF.take(5)

