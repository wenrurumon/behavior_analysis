
rm(list=ls())
setwd('E:\\qianji\\libangqi\\粉丝数据\\粉丝数据')

#########################
# Follow Matrix
#########################

fans1 <- read.csv('fans.follow.count.txt',encoding='UTF-8',header=F)
fans2 <- readLines('fans.p.txt',encoding='UTF-8')
id <- grep('; ',grep('>>> ',fans2,value=T),value=T)
start <- which(fans2%in%id)
end <- c(start[-1]-1,length(start))
fans2.list <- lapply(1:length(start),function(i){
  fans2[start[i]:end[i]]
})
fans2.list <- fans2.list[-4997]

info <- lapply(fans2.list,function(x){
  x[3:(grep('>>>>>> Fans:',x)-1)]
})
like <- lapply(fans2.list,function(x){
  unique(x[(grep('>>>>>> Fans:',x)+1):length(x)])
})
id <- id[-length(id)]
names(info) <- substr(id,5,regexpr('; ',id)-1)
names(like) <- substr(id,5,regexpr('; ',id)-1)
id <- substr(id,5,regexpr('; ',id)-1)[substr(id,5,regexpr('; ',id)-1)%in%fans1[,1]]
fans1 <- fans1[match(id,fans1[,1]),]
info <- info[names(info)%in%id]
like <- like[names(like)%in%id]

like.table <- table(unlist(like))
likes <- names(which(like.table>30))
like.mat <- sapply(like,function(x){likes%in%x})+0
rownames(like.mat) <- likes
follow <- like.mat

##########################
#KOL
##########################

kol <- readLines("sup.txt", encoding='UTF-8')
kol <- strsplit(kol, "<|>", fixed = TRUE)
kol <- t(sapply(kol,function(x){
  if(length(x)==4){x <- c(x,NA)}
  x
}))

kol.name <- do.call(rbind,strsplit(rownames(like.mat),','))
kol <- data.frame(kol.name,kol[match(kol.name[,1],paste(kol[,1])),-1])
colnames(kol) <- c('uid','kolname','follow','fans','countwb','intro')

save(follow,kol,file='E:\\qianji\\libangqi\\data4wb.rda')
