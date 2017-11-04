
sc = SparkContext.getOrCreate()
spark = SparkSession \
    .builder \
    .appName("demo_2") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

start_time = time.time()
#[Info] [start_time: 1509679040.38] 1586033 rows, model training cost time 2411s
from pyspark.mllib.regression import LabeledPoint
model = RandomForestModel.load(sc,"target/tmp/myRandomForestClassificationModel"+"1509679040.38")
model
