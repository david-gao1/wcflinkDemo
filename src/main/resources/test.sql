CREATE TABLE `es070818` (
  `d_id`  INTEGER
  `c1`  STRING,
  `c2`  BIGINT
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = '10.0.9.45:18115',
  'index' = 'es_index098',
  'document-type' = 'es_type099',
  'sink.bulk-flush.max-actions' = '10',
  'failure-handler' = 'ignore',
  'format' = 'json'
);


CREATE TABLE `es070818` (
  `d_id`  INTEGER
  `c1`  STRING,
  `c2`  BIGINT
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = '10.0.9.45:18115',
  'index' = 'es_index098',
  'document-type' = 'es_type099',
  'sink.bulk-flush.max-actions' = '10',
  'failure-handler' = 'ignore',
  'format' = 'json'
);