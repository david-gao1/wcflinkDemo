create table gao_test_052203
( `id` INTEGER NOT NULL,`d_id` INTEGER NOT NULL,
`department` ROW<d_name STRING,d_id BIGINT> NOT NULL,
`messageObject` ROW<message ARRAY<BIGINT>> NOT NULL,
`name` STRING NOT NULL,`personalMessage` ARRAY<BIGINT> NOT NULL,
cloud_wise_proc_time as  proctime(), cloud_wise_event_time as  to_timestamp(FROM_UNIXTIME(`department`.`d_id` / 1000)),
watermark for cloud_wise_event_time as cloud_wise_event_time - INTERVAL '5' SECOND  )
with (
'connector'='kafka-0.11',
'topic'='kafka_source1',
'properties.bootstrap.servers'= 'localhost:9092',
'properties.max.poll.records'= '5000',
'properties.group.id' = 'frist',
'format' = 'json',
'json.ignore-parse-errors' = 'true',
'scan.startup.mode' = 'latest-offset');


create table gao_test_052203121
( `sum_d_id` INTEGER NOT NULL,`d_id` INTEGER NOT NULL,start_time TIMESTAMP(3),end_time TIMESTAMP(3) )
with
('connector'='kafka-0.11','topic'='topic_gao-0518-nest-out',
'properties.bootstrap.servers'= 'localhost:9092','properties.acks'= '1',
'properties.retries' = '3','properties.batch.size' = '1048576',
'properties.linger.ms' = '30','properties.buffer.memory' = '12582912',
'json.ignore-parse-errors' = 'true','format' = 'json');;




insert into gao_test_052203121 select  sum(`d_id`) as `sum_d_id` ,`d_id` as `d_id`,
TUMBLE_START(cloud_wise_proc_time,  INTERVAL '10' SECOND) as start_time,
 TUMBLE_END(cloud_wise_proc_time,  INTERVAL '10' SECOND) as end_time
 from gao_test_052203  group by TUMBLE(cloud_wise_proc_time,  INTERVAL '10' SECOND) ,`d_id`;
