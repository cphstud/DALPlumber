library(httr)
library(dplyr)
library(jsonlite)
library(RMariaDB)
library(DBI)

# conn til db
con=dbConnect(MariaDB(),
              user="root",
              password="OttoRehagel123",
              host="localhost",
              db="bbair")
# fly over bornholm
# box=c(12.327830,55.483425,12.824961,55.816619)



cn=c("icao24","callsign","origin_country","time_position","last_contact","longitude","latitude","baro_altitude","on_ground","velocity", "true_track","vertical_rate","sensors","geo_altitude","squawk", "spi", "category" )
urlb='https://opensky-network.org/api/states/all?lamin=55.483425&lomin=12.327830&lamax=55.816619&lomax=12.824961'
res=GET(url=urlb, authenticate(user = "thorwulf",password = "D5vdhvt!"))

resraw=content(res,as="text")
listres=fromJSON(resraw, flatten = T)
dfres=as.data.frame(listres$states)
colnames(dfres)=cn

# følge flyet - ikke ruten
dficao=dfres %>% select(icao24)
dficaovar=dfres %>% select(-icao24)


dbWriteTable(con,"statesicao",dficao, overwrite=T,append=F)
dbWriteTable(con,"statesvar",dfres, overwrite=T,append=F)

# new run

