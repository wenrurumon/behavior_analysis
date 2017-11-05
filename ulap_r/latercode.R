
library(SparkR)
#Dummydata

n <- 100000
data <- data.frame(real = as.numeric(runif(n,0,1)>=0.5),prediction = runif(n,0,1))
head(data)
save_dataset('/Unilever/Private/Acxiom/dataset/dummy4r',data)

############

#rm(data)
headf <- function(test,n=20){
    showDF(test, numRows = n, truncate = TRUE)
}

raw <- get_dataset('/Unilever/Private/Acxiom/dataset/dummy4r')
headf(raw)

test <- filter(raw,'prediction'>=0.5)
