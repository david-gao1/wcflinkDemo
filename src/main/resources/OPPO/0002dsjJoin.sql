CREATE TABLE cdc1 ( stu_id INT, proc_time AS PROCTIME(), PRIMARY KEY (stu_id) NOT ENFORCED
     ) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = 'localhost',
      'port' = '3306',
      'username' = 'root',
      'password' = '11111111',
      'database-name' = 'dataflow_test',
      'table-name' = 'example1',
      'scan.startup.mode' = 'latest-offset'
     );

CREATE TABLE `w1` (
 id INT,
  `type`  STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/dataflow_test',
  'username' = 'root',
  'password' = '11111111',
  'table-name' = 'test1',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'lookup.cache.max-rows' = '10',
  'lookup.cache.ttl' = '10MINUTE'
);


CREATE TABLE `w2` (
 id INT,
  `type`  STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/dataflow_test',
  'username' = 'root',
  'password' = '11111111',
  'table-name' = 'test2',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'lookup.cache.max-rows' = '10',
  'lookup.cache.ttl' = '10MINUTE'
);


create table gao_test_052203 (  id INT, type STRING,
 PRIMARY KEY (id) NOT ENFORCED)
WITH ('connector'='kafka',
'topic'='source-3',
'properties.bootstrap.servers'= 'localhost:9092',
'properties.max.poll.records'= '1',
'properties.group.id' = 'frist',
'format' = 'debezium-json');

insert into gao_test_052203 SELECT c.stu_id as id ,w2.type as type FROM cdc1 AS c
  left JOIN w1 FOR SYSTEM_TIME AS OF c.proc_time  ON c.stu_id = w1.id
  left join w2 FOR SYSTEM_TIME AS OF c.proc_time  on c.stu_id = w2.id;
