
rm(list=ls())
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
cluster <- function(data,nclust){
  kmeans(data,nclust)$cluster
}

################################
#Main
################################

#Assuming there were 3 patterns
p1 <- runif(10,-.3,.5)
p2 <- runif(10,-.3,.5)
p3 <- runif(10,-.3,.5)

#generate 1000 samples with/without patterns
id_pattern <- lapply(1:1000,function(x){
  sample(0:1,3,T)
})

#how many unique pattern combination we have
length(unique(id_pattern))

#Generate actual behaviour
id_action <- scale(t(sapply(id_pattern,function(x){
  cbind(p1,p2,p3) %*% cbind(x)
})))
#Provide noise on data
id_action <- id_action + runif(length(id_action),-0.2,0.2)

#Finalize Data
id_action <- (id_action>0)
id_pattern <- do.call(rbind,id_pattern)
dimnames(id_action) <- list(paste0('id',1:nrow(id_action)),
                            paste0('cid',1:ncol(id_action)))
dimnames(id_pattern) <- list(paste0('id',1:nrow(id_pattern)),
                             paste0('cid',1:ncol(id_pattern)))

#Clustering
x <- qpca2(id_action)$X
x.clust <- cluster(x,2^(ncol(x)))

#Export Cluster Feature
group_pattern <- sapply(unique(x.clust),function(i){
  colMeans(id_action[x.clust==i,,drop=F])
})/colMeans(id_action)

#Correlation Matrix Between Clusters
heatmap(cor(group_pattern))

#Export Cluster Result
rlt <- dplyr::arrange(data.frame(id_pattern,x.clust),x.clust)
unique(rlt)

