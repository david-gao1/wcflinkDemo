create table kafka_source1
( `d_id` INTEGER NOT NULL,
`department` ROW<d_name STRING,d_id BIGINT> NOT NULL)
with ('connector'='kafka-0.11','topic'='kafka_source1','properties.bootstrap.servers'= 'localhost:9092','properties.max.poll.records'= '5000','properties.group.id' = 'frist','format' = 'json','json.ignore-parse-errors' = 'true','scan.startup.mode' = 'latest-offset');


create table kafka_source2
( `id` INTEGER NOT NULL,
`name` STRING NOT NULL,
`randomNum` INTEGER NOT NULL)
with ('connector'='kafka-0.11','topic'='kafka_source2','properties.bootstrap.servers'= 'localhost:9092','properties.max.poll.records'= '5000','properties.group.id' = 'frist','format' = 'json','json.ignore-parse-errors' = 'true','scan.startup.mode' = 'latest-offset');


create table kafka_sink
( `d_id` INTEGER NOT NULL,
  `department` ROW<d_name STRING,d_id BIGINT> NOT NULL,
  `id` INTEGER NOT NULL,
  `name` STRING NOT NULL,
  `randomNum` INTEGER NOT NULL
   )
with
('connector'='kafka-0.11','topic'='kafka_sink1',
'properties.bootstrap.servers'= 'localhost:9092','properties.acks'= '1',
'properties.retries' = '3','properties.batch.size' = '1048576',
'properties.linger.ms' = '30','properties.buffer.memory' = '12582912',
'json.ignore-parse-errors' = 'true','format' = 'json');




insert into kafka_sink select *
from kafka_source1 left join kafka_source2 on kafka_source1.d_id=kafka_source2.id;

