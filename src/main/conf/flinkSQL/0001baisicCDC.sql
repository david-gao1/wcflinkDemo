CREATE TABLE products ( stu_id INT, user_action_time AS PROCTIME(), PRIMARY KEY (stu_id) NOT ENFORCED
     ) WITH ( 
      'connector' = 'mysql-cdc', 
      'hostname' = '10.0.21.129',
      'port' = '3306', 
      'username' = 'root', 
      'password' = '11111111', 
      'database-name' = 'dataflow_test', 
      'table-name' = 'example1'
     );

create table gao_test_052203 (  stu_id INT, time13 bigint,
 PRIMARY KEY (stu_id) NOT ENFORCED)
WITH ('connector'='kafka',
'topic'='source-1',
'properties.bootstrap.servers'= '10.0.21.129:9092',
'properties.max.poll.records'= '5000',
'properties.group.id' = 'frist',
'format' = 'debezium-json');

insert into gao_test_052203 select stu_id,toMills(user_action_time) as time13 from products;