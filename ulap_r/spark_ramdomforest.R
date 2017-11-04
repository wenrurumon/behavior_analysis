model <- spark.randomForest(test, 
V1 ~ V2+V3+V4+V5+V6+V7+V8+V9+V10+V11+V12+V13+V14+V15+V16+V17+V18+V19+V22+V23+V24+V25
, "classification", numTrees = 20)
pred <- SparkR::predict(model,test)

registerTempTable(out, "table")
headf(sql(sqlContext, "SELECT V1,prediction,V1-prediction as test, count(1) as count FROM table group by V1,prediction,test"))
