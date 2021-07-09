create table flink_test_2_1 (
  id BIGINT,
  day_time VARCHAR,
  amnount BIGINT,
  proctime AS PROCTIME ()
)
 with (
   'connector'='kafka-0.11','topic'='source_1',
   'properties.bootstrap.servers'= 'localhost:9092','properties.acks'= '1',
   'properties.retries' = '3','properties.batch.size' = '1048576',
   'properties.linger.ms' = '30','properties.buffer.memory' = '12582912',
   'json.ignore-parse-errors' = 'true','format' = 'json'
 );


create table flink_test_2_2 (
  id BIGINT,
  coupon_amnount BIGINT,
  proctime AS PROCTIME ()
)
 with (
  'connector'='kafka-0.11','topic'='source_2',
  'properties.bootstrap.servers'= 'localhost:9092','properties.acks'= '1',
  'properties.retries' = '3','properties.batch.size' = '1048576',
  'properties.linger.ms' = '30','properties.buffer.memory' = '12582912',
  'json.ignore-parse-errors' = 'true','format' = 'json'
 );


CREATE TABLE sync_test_2 (
                   day_time string,
                   total_gmv bigint,
                   PRIMARY KEY (day_time) NOT ENFORCED
 ) WITH (
    'connector'='kafka-0.11','topic'='sink_join',
     'properties.bootstrap.servers'= 'localhost:9092','properties.acks'= '1',
     'properties.retries' = '3','properties.batch.size' = '1048576',
     'properties.linger.ms' = '30','properties.buffer.memory' = '12582912',
     'json.ignore-parse-errors' = 'true','format' = 'json'
 );

INSERT INTO sync_test_2 SELECT
  day_time,
  SUM(amnount - coupon_amnount) AS total_gmv FROM
  (
    SELECT a.day_time as day_time,a.amnount as amnount,
      b.coupon_amnount as coupon_amnount FROM
      flink_test_2_1 as a LEFT JOIN flink_test_2_2 b on b.id = a.id
  )
GROUP BY day_time;