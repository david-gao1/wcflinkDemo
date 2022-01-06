-- 1. dosm_work_order_instance 流程工单数据表
-- is_xx 在Flink中是否被认为是Boolean类型，否则怎么会报异常 java.lang.Boolean Cant cast to java.lang.Integer
-- 问题分析： Flink使用jdbc采集MySQL数据时，会将tinyInt(1)和boolean转换为tinyInt
-- 问题处理： 在Flink jdbc 采集数据时，url中新增参数 tinyInt1isBit=false&transformedBitIsBoolean=false
CREATE TABLE dosm_work_order_instance (
  id STRING,
  account_id STRING,
  primary_catalog_id STRING,
  catalog_id STRING,
  model_definition_key STRING,
  model_definition_id STRING,
  process_instance_id INT,
  title STRING,
  urgent_level STRING,
  ticket_desc STRING,
  is_test tinyint,
  is_sub_process tinyint,
  label STRING,
  order_type STRING,
  order_status STRING,
  order_status_value INT,
  source_id INT,
  department_id STRING,
  department_level STRING,
  created_user_id STRING,
  created_time TIMESTAMP ,
  updated_user_id STRING,
  updated_time TIMESTAMP ,
  urged_time TIMESTAMP,
  is_del tinyint,
  vip_level STRING,
  vip_show STRING,
  sub_task_flag tinyint,
  primary_work_order_id STRING,
  primary_node_id STRING,
  is_lock tinyint,
  proc_time AS PROCTIME(),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'mysql-cdc',
   'hostname' = '10.0.10.186',
   'port' = '18103',
   'username' = 'Rootmaster',
   'password' = 'Rootmaster@777',
   'database-name' = 'dosm_activiti',
   'table-name' = 'dosm_work_order_instance',
   'scan.startup.mode' = 'initial');

-- 2. dosm_work_order_node_flow_history 工单流转历史表
CREATE TABLE dosm_work_order_node_flow_history (
  id STRING,
  account_id INT,
  model_definition_key STRING,
  process_instance_id INT,
  execution_id INT,
  task_id INT,
  active_node_id STRING,
  node_type STRING,
  is_active tinyint,
  node_status TINYINT,
  is_rejected tinyint,
  previous_record_id INT,
  previous_node_id STRING,
  created_time TIMESTAMP,
  updated_time TIMESTAMP,
  active_node_name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'dosm_work_order_node_flow_history',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');

-- 3. dosm_model_node_base_info 工单节点信息表
CREATE TABLE dosm_model_node_base_info (
  id STRING,
  model_definition_id STRING,
  active_node_id STRING,
  active_node_name STRING,
  active_node_type STRING,
  handle_type STRING,
  handle_persion_area_type STRING,
  assign_type STRING,
  has_sub_process INT,
  attribute_info STRING,
  created_user_id STRING,
  created_time TIMESTAMP,
  PRIMARY KEY (id) NOT ENFORCED
)WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'dosm_model_node_base_info',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');

-- 4. dosm_work_order_form_data 工单提交表单数据
CREATE TABLE dosm_work_order_form_data (
  id STRING,
  account_id STRING,
  work_order_id STRING,
  plan_start_time STRING,
  plan_end_time STRING,
  work_order_form_data STRING,
  form_data_json_id STRING,
  created_user_id STRING,
  created_time TIMESTAMP,
  updated_user_id STRING,
  updated_time TIMESTAMP,
  PRIMARY KEY (id) NOT ENFORCED
)WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'dosm_work_order_form_data',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');


-- 5. dosm_model_definition 工单表单字段定义字典表
CREATE TABLE dosm_model_definition (
  id STRING,
  account_id STRING,
  model_name STRING,
  model_definition_key STRING,
  model_version INT,
  model_definition_id STRING,
  process_definition_key STRING,
  process_definition_id STRING,
  model_type STRING,
  label_prefix STRING,
  service_icon STRING,
  model_status STRING,
  created_user_id STRING,
  created_time TIMESTAMP,
  updated_user_id STRING,
  updated_time TIMESTAMP,
  group_id STRING,
  PRIMARY KEY (id) NOT ENFORCED
)WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'dosm_model_definition',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');


-- 6. sys_user  用户信息表
 CREATE TABLE sys_user (
  id DECIMAL,
  name STRING ,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/douc_1?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'sys_user',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');


-- 7。定义输出至Kafka
CREATE TABLE dosm_data_report_important_event (
    id  STRING ,
    model_definition_key  STRING ,
    model_definition_id  STRING ,
    process_instance_id  INT ,
    created_user_id  STRING ,
    created_user_name STRING ,
    catalog_id   STRING ,
    primary_catalog_id  STRING ,
    label STRING ,
    created_time TIMESTAMP ,
    updated_time TIMESTAMP ,
    order_status STRING ,
    current_node STRING ,
    problemstate STRING ,
    breakstarttime STRING ,
    liabilitydepartment STRING ,
    liabilitygroup STRING ,
    reasonclass STRING ,
    reasonsubclass STRING ,
    CHorRL STRING ,
    systemname STRING ,
    systemgrade STRING ,
    belongdepartment STRING ,
    systemnumber STRING ,
    affectedinformation STRING ,
    improvemeasurelevel STRING ,
    improvemeasuretype STRING ,
    turnproblem STRING ,
    responsegroup STRING ,
    responseperson STRING ,
    plancompletetime STRING ,
    responsedepartment STRING ,
    infrastructureownership STRING ,
    onelevelservicedirectory STRING ,
    twolevelservicedirectory STRING ,
    unavailabletime STRING ,
    model_name STRING,
    version BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'important_event_11',
    'properties.bootstrap.servers' = '10.0.12.213:18108',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'maxwell-json');


-- 8. 使用自定义udf写入数据
INSERT INTO dosm_data_report_important_event
 SELECT
    wt.id as id ,
    wt.model_definition_key as model_definition_key ,
    wt.model_definition_id as model_definition_id ,
    wt.process_instance_id as process_instance_id ,
    wt.created_user_id as created_user_id ,
    wt.created_user_name as created_user_name,
    wt.catalog_id  as catalog_id  ,
    wt.primary_catalog_id as primary_catalog_id ,
    wt.label as label,
    wt.created_time as  created_time,
    wt.updated_time as  updated_time,
    ck_transform(wt.order_status,array['FINISHED','PROCESSING','BE_PROCESSED','CLOSED'],array['已完成','处理中','待领取','已关闭'],'其他') as order_status,
    wt.current_node AS current_node,
    problemstate,
    breakstarttime,
    liabilitydepartment,
    liabilitygroup,
    reasonclass,
    reasonsubclass,
    CHorRL,
    systemname,
    systemgrade,
    belongdepartment,
    systemnumber,
    affectedinformation,
    improvemeasurelevel,
    improvemeasuretype,
    turnproblem,
    responsegroup,
    responseperson,
    plancompletetime,
    responsedepartment,
    infrastructureownership,
    onelevelservicedirectory,
    twolevelservicedirectory,
    unavailabletime,
    wt.model_name as model_name,
    addVersion(wt.proc_time) as version
 from (
    select
        workOrder.id as id ,
        workOrder.model_definition_key as model_definition_key ,
        workOrder.model_definition_id as model_definition_id ,
        workOrder.process_instance_id as process_instance_id ,
        workOrder.created_user_id as created_user_id ,
        suser.name as created_user_name,
        workOrder.catalog_id  as catalog_id  ,
        workOrder.primary_catalog_id as primary_catalog_id ,
        workOrder.label as label,
        workOrder.created_time as  created_time,
        workOrder.updated_time as  updated_time,
        workOrder.order_status as order_status,
        nodeBaseInfo.active_node_name AS current_node,
        nodeFormData.work_order_form_data as form_data,
        workOrder.model_name as model_name,
        workOrder.proc_time as proc_time
     from (
            select
                wo.id as id,
                wo.model_definition_key as model_definition_key ,
                wo. model_definition_id as model_definition_id,
                wo.process_instance_id as process_instance_id,
                wo.created_user_id as created_user_id,
                wo.catalog_id  as catalog_id,
                wo.primary_catalog_id as primary_catalog_id,
                wo.label as label,
                wo.created_time as created_time,
                wo.updated_time as updated_time,
                wo.order_status as order_status,
                wo.proc_time as proc_time,
                modelDefin.model_name as model_name
            from  dosm_work_order_instance wo
            INNER JOIN dosm_model_definition FOR SYSTEM_TIME AS OF wo.proc_time modelDefin on modelDefin.model_definition_id = wo.model_definition_id and modelDefin.model_name like '%重大事件%'
            where wo.is_del = 0 and wo.is_test = 0
        ) workOrder
     LEFT JOIN sys_user FOR SYSTEM_TIME AS OF workOrder.proc_time suser on suser.id = CAST(workOrder.created_user_id as INT)
     LEFT JOIN dosm_work_order_node_flow_history FOR SYSTEM_TIME AS OF workOrder.proc_time  nodeHistory  on nodeHistory.process_instance_id = workOrder.process_instance_id and nodeHistory.node_status in (1, 2)
     LEFT JOIN dosm_model_node_base_info FOR SYSTEM_TIME AS OF workOrder.proc_time nodeBaseInfo   on workOrder.model_definition_id = nodeBaseInfo.model_definition_id  and nodeBaseInfo.active_node_id = nodeHistory.active_node_id
     left join dosm_work_order_form_data FOR SYSTEM_TIME AS OF workOrder.proc_time nodeFormData on nodeFormData.work_order_id=workOrder.id
) wt
    LEFT JOIN LATERAL TABLE(impFlattenFormData(handlerFormData(wt.form_data ,wt.model_definition_id))) AS
    tb(problemstate,breakstarttime,liabilitydepartment,liabilitygroup,reasonclass,reasonsubclass,CHorRL,
    systemname,systemgrade,belongdepartment,systemnumber,affectedinformation,
    improvemeasurelevel,improvemeasuretype,turnproblem,responsegroup,responseperson,plancompletetime,responsedepartment,
    infrastructureownership,onelevelservicedirectory,twolevelservicedirectory,unavailabletime) on true;