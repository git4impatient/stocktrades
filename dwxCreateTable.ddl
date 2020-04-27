create table sampletxt2 stored as textfile   TBLPROPERTIES('transactional'='false') 
as select * from sample_07;
