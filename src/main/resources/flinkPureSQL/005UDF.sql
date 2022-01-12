CREATE TABLE `gao_sink` (
  str String NOT NULL,
  newWord String,
  newLength int,
  proc_time bigint
) WITH (
  'connector' = 'kafka-0.11',
  'topic' = 'topic_gao-out',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

CREATE TABLE `gao_source` (
  `str`  String NOT NULL,
  `proc_time` AS `proctime`()
) WITH (
  'connector' = 'kafka-0.11',
  'topic' = 'source-1',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.max.poll.records' = '5000',
  'properties.group.id' = 'frist',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'scan.startup.mode' = 'latest-offset'
);


insert into gao_sink select str,newWord,newLength,to13mills(proc_time) as proc_time  from gao_source
 LEFT JOIN LATERAL TABLE(FlattenExt(str,'newWord string,newLength int')) AS T(newWord, newLength) ON TRUE;

