# coding=utf-8

import numpy as np
import pandas as pd
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils


sc = SparkContext.getOrCreate()
spark = SparkSession \
    .builder \
    .appName("demo") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

start_time = time.time()

dataset = get_dataset("", proxy=False)
        
print("[Info] %s rows cost time %ss" % (dataset.count(), int(time.time() - start_time)))
start_time = time.time()

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

