start_time = time.time()
#[Info] [start_time: 1509679040.38] 1586033 rows, model training cost time 2411s
from pyspark.mllib.regression import LabeledPoint


TRESHOLD_TO_FILTER = 0.1

df_0 =  trainset.filter( trainset.label < 0.5).sample(True, TRESHOLD_TO_FILTER, 29)
df_1 =  trainset.filter( trainset.label > 0.5)

trainset_sample = df_0.union(df_1)
#trainset_sample.groupBy('label').count().show()

mydf = trainset_sample.rdd.map(lambda x: LabeledPoint(x[-1], x[:-1]))


# Load and parse the data file into an RDD of LabeledPoint.
#data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt')

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = mydf.randomSplit([0.7, 0.3])

idx_start = 263
categoricalFeatureInfo = {
    idx_start + 0:4,
    idx_start + 1:4,
    idx_start + 2:5,
    idx_start + 3:4,
    idx_start + 4:4,
    idx_start + 5:5,
    idx_start + 6:4,
    idx_start + 7:4,
    idx_start + 8:5,
    idx_start + 9:9,
    idx_start + 10:3,
    idx_start + 11:7,
    idx_start + 12:3,
    idx_start + 13:5,
    idx_start + 14:9,
    idx_start + 15:32,
    #idx_start + 16:362,
    #idx_start + 17:3,
    #idx_start + 18:3,
    idx_start + 16:6,
    idx_start + 17:7,
    idx_start + 18:6,
    idx_start + 19:6
}


model = RandomForest.trainRegressor(trainingData, 
    categoricalFeaturesInfo=categoricalFeatureInfo,
    numTrees=100, 
    featureSubsetStrategy="auto",
    impurity='variance', 
    maxDepth=10,
    maxBins=40)

#print('Learned classification forest model:')
#print(model.toDebugString())

# Save and load model
model.save(sc, "target/tmp/myRandomForestClassificationModel"+ str(start_time))  #1509679040.38

# sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel"+"--------")

print("[Info] [start_time: %s] %s rows, model training cost time %ss" % ( start_time, trainset_sample.count(), int(time.time() - start_time)))

