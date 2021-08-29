create table sls_test_single_local (
                                     id INT,
                                     name VARCHAR,
                                     age BIGINT,
                                     rate FLOAT
                                   )
WITH ('connector'='kafka-0.11',
'topic'='topic_gao-out',
'properties.bootstrap.servers'= 'localhost:9092',
'properties.acks'= '1',
'properties.retries' = '3',
'properties.batch.size' = '1048576',
'properties.linger.ms' = '30',
'properties.buffer.memory' = '12582912',
'json.ignore-parse-errors' = 'true','format' = 'json');


CREATE TEMPORARY TABLE clickhouse_output (
  id INT PRIMARY KEY NOT ENFORCED,
  name VARCHAR,
  age BIGINT,
  rate FLOAT
) WITH (
  'connector' = 'clickhouse',
  'url' = 'jdbc:clickhouse://localhost:8123',
  'username' = 'test',
  'password' = '280226Ck',
  'table-name' = 'sls_test_single_local'
);

INSERT INTO clickhouse_output
 SELECT
  id,
  name,
  age,
  rate FROM sls_test_single_local;