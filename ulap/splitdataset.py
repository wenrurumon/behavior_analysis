start_time = time.time()
#[Info] [start_time: 1509679040.38] 1586033 rows, model training cost time 2411s
from pyspark.mllib.regression import LabeledPoint
TRESHOLD_TO_FILTER = 0.1
df_0 =  trainset.filter( trainset.label < 0.5).sample(True, TRESHOLD_TO_FILTER, 29)
df_1 =  trainset.filter( trainset.label > 0.5)

trainset_sample = df_0.union(df_1)
#trainset_sample.groupBy('label').count().show()

mydf = trainset_sample.rdd.map(lambda x: LabeledPoint(x[-1], x[:-1]))
(trainingData, testData) = mydf.randomSplit([0, 1])

print(trainset_sample.count())
print(testData.count())
