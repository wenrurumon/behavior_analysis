rm(list=ls())
load('E:\\qianji\\libangqi\\data4wb.rda')
setwd('E:\\qianji\\libangqi\\')
load('test.rda')

#Presenter

clust <- clust.simlr$y
f <- t(follow)
f <- f[rowSums(f)>0,]
f2 <- lapply(1:20,function(i){
  x <- f[clust$cluster==i,,drop=F]
  feature <- colMeans(x)
  list(x=x,feature=feature)
})
centers <- sapply(f2,function(x){x$feature})
f2.present <- lapply(f2,function(i){
  x <- cor(t(i$x),i$feature)
  x[is.na(x)] <- -1
  (x[order(-x),])
})
f2.present <- unlist(f2.present)
f.clust <- data.frame(uid = rownames(f),
                      clust = clust$cluster,
                      cor = f2.present[match(rownames(f),names(f2.present))])
write.csv(f.clust,'cluster_output.csv')

#centers

kol.sel <- as.numeric(kol$follow)/as.numeric(kol$fans)
# mean(paste(kol$uid,kol$kolname,sep=',')==colnames(f))
centers <- cbind(centers / rowMeans(t(f)),rowMeans(t(f)),kol.sel)
colnames(centers) <- c(paste0('group',1:20),'libang','all')
write.csv(centers,'centers.csv')
