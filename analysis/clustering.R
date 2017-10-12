rm(list=ls())
load('E:\\qianji\\libangqi\\data4wb.rda')

########################
# Function
########################

qpca <- function(A,scale=T,rank=0){
  if(scale){A <- scale(A)}
  A.svd <- svd(A)
  if(rank==0){
    d <- A.svd$d
  } else {
    d <- A.svd$d-A.svd$d[min(rank+1,nrow(A),ncol(A))]
  }
  d <- d[d > 1e-10]
  r <- length(d)
  prop <- d^2; prop <- cumsum(prop/sum(prop))
  d <- diag(d,length(d),length(d))
  u <- A.svd$u[,1:r,drop=F]
  v <- A.svd$v[,1:r,drop=F]
  x <- u%*%sqrt(d)
  y <- sqrt(d)%*%t(v)
  z <- x %*% y
  rlt <- list(rank=r,X=x,Y=y,Z=x%*%y,prop=prop)
  return(rlt)
}
qpca2 <- function(A,prop1=0.9){
  rank1 <- qpca(A,T,0)$prop
  qpca(A,T,rank=which(rank1>=prop1)[1])
}

follow <- follow[,colSums(follow)>0]
follow.pca <- qpca2(t(follow),0.8)
# f2 <- follow.pca$X[,1:which(follow.pca$prop>=0.9)[1],drop=F]
f2 <- follow.pca$Z
rownames(f2) <- colnames(follow)

############################
#SIMLR
############################

library(SIMLR)
library(data.table)
library(igraph)
clust.simlr <- SIMLR(X = f2, c = 20, cores.ratio = 0)
save(clust.simlr,file='rlt_simlr_follow.rda')

############################
#Clustering Output
############################

rm(list=ls())
setwd('E:\\qianji\\libangqi\\')
load('data4wb.rda')
load('rlt_simlr_follow.rda')
clust <- clust.simlr$y$cluster

test <- sapply(1:20,function(i){
  rowMeans(follow[,clust.simlr$y$cluster==i,drop=F])
})
test <- test/rowMeans(follow)

out <- lapply(1:20,function(i){
  x <- test[test[,i]>1,i]
  cbind(sort(x,T))
})

