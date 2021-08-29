create table gao_test_08 (
`id` INTEGER NOT NULL ,
`deptname` CHAR(50) ,
`emp_id` INTEGER
 )
WITH ('connector'='kafka-0.11',
'topic'='source-1',
'properties.bootstrap.servers'= 'localhost:9092',
'properties.acks'= '1',
'properties.retries' = '3',
'properties.batch.size' = '1048576',
'properties.linger.ms' = '30',
'properties.buffer.memory' = '12582912',
'json.ignore-parse-errors' = 'true',
'format' = 'json');


insert into mypg.flinktest.department select * from gao_test_08;


