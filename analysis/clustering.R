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
  qpca(A,T,rank=which(rank1>=0.9)[1])
}

follow <- follow[,colSums(follow)>0]
follow.pca <- qpca2(follow,0.8)

############################
#SIMLR
############################

library(SIMLR)
library(data.table)
library(igraph)
clust.simlr <- SIMLR(X = (follow), c = 20, cores.ratio = 0)
save(clust.simlr,file='rlt_simlr_follow.rda')


############################
#
############################

followmap <- dplyr::filter(reshape::melt(follow),value!=0)
colnames(followmap) <- c('kol','follow','value')
kolmat <- sqldf::sqldf(
  'select a.kol as akol, b.kol as bkol, 
  sum(a.value) as aval, sum(b.value) as bval
  from followmap a 
  left join followmap b 
  on a.follow=b.follow
  group by akol, bkol'
)

kollist <- unique(kolmat$akol)
kolmat2 <- sapply(kollist,function(koli){
  x <- dplyr::filter(kolmat,akol==koli)[,2:3]
  x[match(kollist,x$bkol),2]
})
kolmat2[is.na(kolmat2)] <- 0
rownames(kolmat2) <- colnames(kolmat2)

test <- igraph::graph_from_adjacency_matrix(kolmat2)
