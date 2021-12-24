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

create table gao_test_052203 ( `id` INTEGER NOT NULL,  cloud_wise_proc_time as  proctime() )
WITH ('connector'='kafka-0.11',
'topic'='topic_gao-in',
'properties.bootstrap.servers'= 'localhost:9092',
'properties.max.poll.records'= '5000',
'properties.group.id' = 'frist',
'format' = 'json',
'json.ignore-parse-errors' = 'true',
'scan.startup.mode' = 'latest-offset');


CREATE TABLE `single_message` (
  `type`  STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/dataflow_test',
  'username' = 'Rootmaster',
  'password' = 'Rootmaster@777',
  'table-name' = 'test',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'lookup.cache.max-rows' = '10',
  'lookup.cache.ttl' = '10MINUTE'
);

insert into
SELECT o.order_id, o.total, c.country, c.zip
FROM Orders AS o
  JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
    ON o.customer_id = c.id;