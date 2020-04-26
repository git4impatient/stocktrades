# (c) copyright 2012 martin lurie - sample code not supported
# all rights reserved   may not be copied or published without permission

impala-shell <<eoj
drop table if exists nysetrades;
create external table nysetrades (
MsgSeqNum int,
MsgType int,
OriginalTradeRefNum int,
SourceSeqNum int,
SourceSessionID int,
SendTime int,
SourceTime int,
StockSymbol string,
PriceNumerator int,
PriceScaleCode int,
Volume int,
ExchangeID string,
SecurityType string,
LinkID int,
TradeCond1 string,
TradeCond2 string,
TradeCond3 string
)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
LOCATION
  'hdfs://gromit.lurie.biz:8020/user/marty/kafkachannel/'
;

select * from nysetrades limit 5; 
eoj


impala-shell <<eoj
invalidate metadata;

drop table if exists nyse_elt;
create table nyse_elt  stored as parquet
as
select 
stocksymbol, sourcetime, pricenumerator, 
pricenumerator/cast (
  concat ('1e', cast(pricescalecode as string) ) as float) unitprice  , 
  volume 
  
from nysetrades 
where sourcetime is not NULL
limit 5000
;

select * from nyse_elt limit 5;
eoj

exit
