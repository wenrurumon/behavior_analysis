
library(SparkR)
#Dummydata

n <- 100000
data <- data.frame(real = as.numeric(runif(n,0,1)>=0.5),prediction = runif(n,0,1))
jdata <- data.frame(uid = paste0('U',round(runif(n,0,1)*1000000000)), gender = runif(n,0,1)>=.5)
head(data)
head(jdata)

save_dataset('/Unilever/Private/Acxiom/dataset/dummy4r',data)
save_dataset('/Unilever/Private/Acxiom/dataset/dummy4r2',jdata)

##################################################################
##################################################################
##################################################################

#rm(data)
headf <- function(test,n=20){
    showDF(test, numRows = n, truncate = TRUE)
}
#########################
#load data
score <- get_dataset('/Unilever/Private/Acxiom/dataset/dummy4r')
udata <- get_dataset('/Unilever/Private/Acxiom/dataset/dummy4r')
headf(udata)
headf(score)

#########################
#hist
if(!('sc'%in%ls())){
    sc <- sparkR.init()
    sqlContext <- sparkRSQL.init(sc)
}

registerTempTable(score, "table")
test <- sql(
    sqlContext, 
    "SELECT real, prediction, round(prediction,2) as hist
     FROM table"
    )
headf(test,10)

registerTempTable(test, "table")
test <- sql(
    sqlContext, 
    "SELECT count(1) as count, hist
     FROM table
     group by hist"
)
headf(test,nrow(test))

#########################
#hist
