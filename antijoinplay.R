library(DBI)
library(stringr)
library(MariaDB)
library(dplyr)

con <- dbConnect(MariaDB,
                 db="play",
                 user=root,
                 host="localhost",
                 password=sys.getenv("mypw")
                 )

isdb=dbListTables(con)
# her mangler en linje
  dfo=dbReadTable("playdf",con)
  dfor=dfo %>% select(-sold)
  mynew=dfnew(dfor)
  cv=colnames(dfor)
  
  #new ones
  newc=anti_join(mynew,dfor, by="idx")
  newc$sold=0
  
  # old ones gone
  oldc=anti_join(mynew, dfor, by="idx")
  oldc$sold=1
  # changed prices?
  mdf=inner_join(mynew,dfor, by="idx") %>% filter(price.x!=price.x) %>% select(sdate.x,idx,price.x,milage.x)
  colnames(mdf)=cv
  
  # update db
  #update price
  for (i in (1:nrow(mdf))) {
    q=sprintf("update table playdf set price = %.0f,sdate='%s' where idx = %.0f",mdf[i,'price'], mdf[i,'sdate'],mdf[i,'idx'])
    rows=dbExecute(con,q)
  }
  #add new ones 
  dbWriteTable(con,"playdf",newc,overwrite=T)
  #update sold ones
  for (i in (1:nrow(oldc))) {
    q=sprintf("update playdf set sold = T where idx = %.0f",oldc[i,'idx'])
    rows=dbExecute(con,q)
  }
} else {
  initDB()
  upq="ALTER TABLE `play`.`playdf` CHANGE COLUMN `idx` `idx` INT NOT NULL ,
  ADD PRIMARY KEY (`idx`)"
  res=dbExecute(upq,con)
}


dfnew <- function(df,n) {
  dfretval=df
  # remove
  idx=sample(1:nrow(df),n)
  dfretval=dfretval[-(idx),]
  # lower price with 25 and 30 % in 1 and 2 row
  dfretval[1,3]=dfretval[1,3]*0.25
  dfretval[2,3]=dfretval[1,3]*0.30
  # add n new rows
  newdate=dfretval[1,1]-1
  for (i in (1:n)) {
    price=round(rnorm(1, mean=900,290),0)*100
    milage=round(rnorm(1, mean=300,100),0)*100
    idx=as.integer(runif(1,4000,5000))
    rowv = data.frame(sdate=newdate,idx=idx,price=price,milage=milage)
    dfretval= cbind(dfretval,rowv)
  }
  return(dfretval)
}

initDB <- function() {
  pv=round((rnorm(20, mean=900,290)),0)
  pv=pv*100
  milage=round(rnorm(20, mean=300,100),0)
  milage=milage*100
  df1=data.frame(sdate=as.Date("2024-10-01"),idx=as.integer(runif(20,3000,4000)),price=as.integer(pv),milage=as.integer(milage))
  df1$sold=F
  dbWriteTable(con,"playdf",df1,overwrite=T)
} 
