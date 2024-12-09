library(DBI)
library(stringr)
library(RMariaDB)
library(dplyr)

con <- dbConnect(MariaDB(),
                 db="play",
                 user="root",
                 host="localhost",
                 password=Sys.getenv("playdbpw")
                 )

isdb=dbListTables(con)
if ("playdfv" %in% isdb) {
  dfo=dbReadTable(con,"playdf")
  dfor=dfo %>% select(-sold)
  mynew=dfnew(dfor,2)
  cv=colnames(dfor)
  
  #new ones
  newc=anti_join(mynew,dfor, by="idx")
  newc$sold=0
  
  # old ones gone
  oldc=anti_join(dfor,mynew, by="idx")
  oldc$sold=1
  # changed prices?
  mdf=inner_join(mynew,dfor, by="idx") %>% filter(price.x!=price.y) 
  mdf=inner_join(mynew,dfor, by="idx") %>% filter(price.x!=price.y) %>% select(-c(milage.y,sdate.y,price.y))
  colnames(mdf)=cv
  mdf
  # update db
  #update price
  for (i in (1:nrow(mdf))) {
    q=sprintf("update playdfv set price = %.0f,sdate='%s' where idx = %.0f",mdf[i,'price'], mdf[i,'sdate'],mdf[i,'idx'])
    print(q)
    rows=dbExecute(con,q)
  }
  #add new ones 
  newc1=newc %>% select(-c(price,sold,sdate))
  newc2=newc %>% select(-milage)
  dbWriteTable(con,"playdfiv",newc1,append=T)
  dbWriteTable(con,"playdfv",newc2,append=T)
  #update sold ones
  for (i in (1:nrow(oldc))) {
    q=sprintf("update playdfv set sold = 1 where idx = %.0f",oldc[i,'idx'])
    rows=dbExecute(con,q)
  }
} else {
  initDB()
  upq="ALTER TABLE `play`.`playdfiv` CHANGE COLUMN `idx` `idx` INT NOT NULL ,
  ADD PRIMARY KEY (`idx`)"
  res=dbExecute(con,upq)
  upq2="ALTER TABLE `play`.`playdfv` 
ADD COLUMN `v_id` INT NOT NULL AUTO_INCREMENT AFTER `sold`,
ADD PRIMARY KEY (`v_id`)"
  res=dbExecute(con,upq2)
  vq="create view playdf as
select iv.idx,iv.milage,v.sdate,v.price,v.sold from playdfiv iv, playdfv v
where iv.idx=v.idx
and v.sold=0"
  res=dbExecute(con,vq)
}


dfnew <- function(df,n) {
  dfretval=df
  # remove
  idx=sample(1:nrow(df),n)
  dfretval=dfretval[-(idx),]
  # lower price with 25 and 30 % in 1 and 2 row
  dfretval[1,'price']=dfretval[1,'price']*0.75
  dfretval[2,'price']=dfretval[1,'price']*0.70
  # add n new rows
  newdate=dfretval[1,'sdate']+1
  for (i in (1:n)) {
    price=round(rnorm(1, mean=900,290),0)*100
    milage=round(rnorm(1, mean=300,100),0)*100
    idx=as.integer(runif(1,4000,5000))
    rowv = data.frame(sdate=newdate,idx=idx,price=price,milage=milage)
    dfretval= rbind(dfretval,rowv)
  }
  return(dfretval)
}

initDB <- function() {
  pv=round((rnorm(20, mean=900,290)),0)
  pv=pv*100
  milage=round(rnorm(20, mean=300,100),0)
  milage=milage*100
  idx=as.integer(sample(3000:4000,20))
  df1=data.frame(sdate=as.Date("2024-10-01"),idx=idx,price=as.integer(pv),milage=as.integer(milage))
  df1$sold=F
  df11=df1 %>% select(idx,milage)
  df12=df1 %>% select(idx,sdate,price,sold)
  dbWriteTable(con,"playdfiv",df11,overwrite=T)
  dbWriteTable(con,"playdfv",df12,overwrite=T)
} 
