create table gao_test_052203121 ( `id` INTEGER NOT NULL  )
WITH ('connector'='kafka-0.11',
'topic'='topic_gao-out',
'properties.bootstrap.servers'= 'localhost:9092',
'properties.acks'= '1',
'properties.retries' = '3',
'properties.batch.size' = '1048576',
'properties.linger.ms' = '30',
'properties.buffer.memory' = '12582912',
'json.ignore-parse-errors' = 'true','format' = 'json');



create table gao_test_052203 ( `id` ,INTEGER NOT NULL,  cloud_wise_proc_time as  proctime() )
WITH ('connector'='kafka-0.11',
'topic'='topic_gao-in',
'properties.bootstrap.servers'= 'localhost:9092',
'properties.max.poll.records'= '5000',
'properties.group.id' = 'frist',
'format' = 'json',
'json.ignore-parse-errors' = 'true',
'scan.startup.mode' = 'latest-offset');

insert into gao_test_052203121 select `id` as `id`  from gao_test_052203;


