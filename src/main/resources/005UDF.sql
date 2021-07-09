CREATE TABLE `gao_test_052203121` (
  `id`  INTEGER NOT NULL
) WITH (
  'connector' = 'kafka-0.11',
  'topic' = 'topic_gao-out',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.acks' = '1',
  'properties.retries' = '3',
  'properties.batch.size' = '1048576',
  'properties.linger.ms' = '30',
  'properties.buffer.memory' = '12582912',
  'json.ignore-parse-errors' = 'true',
  'format' = 'json'
);

CREATE TABLE `gao_test_052203` (
  `d_id`  INTEGER NOT NULL,
  `cloud_wise_proc_time` AS `proctime`()
) WITH (
  'connector' = 'kafka-0.11',
  'topic' = 'kafka_source1',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.max.poll.records' = '5000',
  'properties.group.id' = 'frist',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'scan.startup.mode' = 'latest-offset'
);


SELECT substrtest(d_id,1,3) AS `id` FROM `gao_test_052203`;

