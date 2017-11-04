#[Info] 9347608 rows cost time 214s
# prepare data for training

start_time = time.time()

from pyspark.ml.feature import StringIndexer


# string --> category

# !!!!!!!!!!!!!! just for debug
# dataset = dataset.sample(False, 0.01, 43)

# index string variable
trainset = StringIndexer(inputCol = stringList[0], outputCol = ''.join((stringList[0], '_'))).fit(dataset).transform(dataset)


for x in stringList[1:]:
    trainset = StringIndexer(inputCol = x, outputCol = ''.join((x, '_'))).fit(trainset).transform(trainset)

# create y at last column
from pyspark.sql.functions import udf
from pyspark.sql.types import *

udfMax = udf(lambda x1, x2: max(x1, x2), IntegerType())

trainset = trainset.withColumn('label', udfMax(trainset[objectValues['xifa']], trainset[objectValues['hufa']]))

# prune trainset
trainset = trainset.drop('user_hash')
trainset = trainset.drop('has_car_')
trainset = trainset.drop('has_child_')
trainset = trainset.drop('city_')


for k,v in objectValues.items():
    trainset = trainset.drop(v)


for x in stringList:
    trainset = trainset.drop(x)

trainset.show(5)

print("[Info] %s rows cost time %ss" % (trainset.count(), int(time.time() - start_time)))
