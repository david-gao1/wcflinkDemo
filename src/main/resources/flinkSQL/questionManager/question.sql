-- 1. dosm_work_order_instance 流程工单数据表
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
  is_test TINYINT,
  is_sub_process TINYINT,
  label STRING,
  order_type STRING,
  order_status STRING,
  order_status_value INT,
  source_id INT,
  department_id STRING,
  department_level STRING,
  created_user_id STRING,
  created_time TIMESTAMP ,
  updated_user_id BIGINT,
  updated_time TIMESTAMP ,
  urged_time TIMESTAMP,
  is_del TINYINT,
  vip_level STRING,
  vip_show STRING,
  sub_task_flag TINYINT,
  primary_work_order_id STRING,
  primary_node_id STRING,
  is_lock TINYINT,
  proc_time AS PROCTIME(),
  event TIMESTAMP_LTZ( 3 ) METADATA FROM  'op_ts' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'mysql-cdc',
   'hostname' = '10.0.10.186',
   'port' = '18103',
   'username' = 'Rootmaster',
   'password' = 'Rootmaster@777',
   'database-name' = 'dosm_activiti_oppo',
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
  is_rejected TINYINT,
  previous_record_id INT,
  previous_node_id STRING,
  created_time TIMESTAMP,
  updated_time TIMESTAMP,
  active_node_name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti_oppo?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
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
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti_oppo?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
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
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti_oppo?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
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
  is_del INT,
  group_id STRING,
  PRIMARY KEY (id) NOT ENFORCED
)WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti_oppo?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'dosm_model_definition',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');


-- 6. dosm_work_order_node_comment 节点表
CREATE TABLE dosm_work_order_node_comment (
  id STRING,
  account_id INT,
  model_definition_key STRING,
  process_instance_id INT,
  order_type STRING,
  sequence TINYINT,
  user_id INT,
  task_id INT,
  active_node_id STRING,
  active_node_name STRING,
  is_first_user_task TINYINT,
  flow_time INT,
  node_info STRING,
  approve_field_data STRING,
  data_type TINYINT,
  entrust_id INT,
  entrust_type STRING,
  created_time TIMESTAMP,
  created_user_id INT,
  updated_time TIMESTAMP,
  updated_user_id INT,
  sign_user_id STRING,
  PRIMARY KEY (id) NOT ENFORCED
)WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti_oppo?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'dosm_work_order_node_comment',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');

-- 7. sys_user  用户信息表
 CREATE TABLE sys_user (
  id BIGINT,
  name STRING ,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/douc_oppo?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'sys_user',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');

-- 8。sys_group  用户组表
CREATE TABLE sys_group (
  id BIGINT,
  name STRING,
  description STRING,
  parent_id BIGINT,
  level STRING,
  sequence INT,
  account_id BIGINT,
  create_user_id BIGINT,
  create_time TIMESTAMP,
  modify_user_id BIGINT,
  modify_time TIMESTAMP ,
  PRIMARY KEY (id) NOT ENFORCED
)WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/douc_oppo?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'sys_group',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');

-- 9。sys_group_user 用户与群组对应表
CREATE TABLE sys_group_user (
  id BIGINT,
  group_id BIGINT,
  user_id BIGINT,
  account_id BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
)WITH ('connector' ='jdbc',
'url' = 'jdbc:mysql://10.0.10.186:18103/douc_oppo?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false&transformedBitIsBoolean=false',
'driver' = 'com.mysql.cj.jdbc.Driver',
'table-name' = 'sys_group_user',
'username' = 'Rootmaster',
'password' = 'Rootmaster@777');


-- 10.定义输出至Kafka
CREATE TABLE dosm_data_report_question_manager (
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
    finish_time STRING,
    plan_start_time STRING,
    plan_end_time STRING,
    order_status STRING ,
    current_node STRING ,
    current_handle_user STRING,

    submit_time STRING ,
    handle_group STRING ,
    handle_user STRING ,
    handle_user_id_h STRING ,
    handle_user_id_a STRING ,

    applicant STRING,
    approvalopinion STRING,
    bizType STRING,
    checkopinion STRING,
    checkresult STRING,
    checkresults STRING,
    defectordemandID STRING,
    defectsource STRING,
    defectstatus STRING,
    defecttype STRING,
    eABHbYMDgVJvrQM STRING,
    OPPOcloudmainclass STRING,
    OPPOcloudsubclass STRING,
    PMapproval STRING,
    problemstate STRING,
    product STRING,
    productLeader STRING,
    productLine STRING,
    productmanager STRING,
    projectlist STRING,
    rootcauseanalysis STRING,
    sCddFfHQgEbqvzQ STRING,
    solvedOverdueTime STRING,
    source STRING,
    systemascription STRING,
    systemLevel STRING,
    systemname STRING,
    systemnumber STRING,

    plancompletetime STRING,
    solvedOverdueTime_t STRING,
    problemstatus STRING,

    model_name STRING,
    verion BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'question_manager',
    'properties.bootstrap.servers' = '10.0.12.213:18108',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'maxwell-json');

-- 11. 使用自定义udf
insert into dosm_data_report_question_manager
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
    IF(wt.order_status = 'FINISHED',CAST(wt.updated_time as STRING),'') as finish_time,
    wt.plan_start_time as plan_start_time,
    wt.plan_end_time as plan_end_time,
    ck_transform(wt.order_status,array['FINISHED','PROCESSING','BE_PROCESSED','CLOSED'],array['已完成','处理中','待领取','已关闭'],'其他') as order_status,
    wt.current_node AS current_node,
    wt.current_handle_user as current_handle_user,

    submit_time,
    handle_group,
    handle_user,
    handle_user_id_h,
    handle_user_id_a,
    applicant,
    approvalopinion,
    bizType,
    checkopinion,
    checkresult,
    checkresults,
    defectordemandID,
    defectsource,
    defectstatus,
    defecttype,
    eABHbYMDgVJvrQM,
    OPPOcloudmainclass,
    OPPOcloudsubclass,
    PMapproval,
    problemstate,
    product,
    productLeader,
    productLine,
    productmanager,
    projectlist,
    rootcauseanalysis,
    sCddFfHQgEbqvzQ,
    solvedOverdueTime,
    source,
    systemascription,
    systemLevel,
    systemname,
    systemnumber,
    plancompletetime,
    solvedOverdueTime_t,
    problemstatus,

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
        nodeFormData.plan_start_time as plan_start_time,
        nodeFormData.plan_end_time as plan_end_time,
        modelDefin.model_name as model_name,
        nm.current_handle_user as current_handle_user,
        workOrder.proc_time as proc_time
     from (
            select
                wo.id as id,
                wo.model_definition_key as model_definition_key ,
                wo.model_definition_id as model_definition_id,
                wo.process_instance_id as process_instance_id,
                wo.created_user_id as created_user_id,
                wo.catalog_id  as catalog_id,
                wo.primary_catalog_id as primary_catalog_id,
                wo.label as label,
                wo.created_time as created_time,
                wo.updated_time as updated_time,
                wo.order_status as order_status,
                wo.proc_time as proc_time
            from  dosm_work_order_instance wo
            where wo.is_del = 0 and wo.is_test = 0
        ) workOrder
     INNER JOIN dosm_model_definition FOR SYSTEM_TIME AS OF workOrder.proc_time modelDefin on modelDefin.model_definition_id = workOrder.model_definition_id and modelDefin.model_name like '%问题管理%'
     LEFT JOIN sys_user FOR SYSTEM_TIME AS OF workOrder.proc_time suser on suser.id  = CAST(workOrder.created_user_id as BIGINT)
     LEFT JOIN dosm_work_order_node_flow_history FOR SYSTEM_TIME AS OF workOrder.proc_time  nodeHistory  on nodeHistory.process_instance_id = workOrder.process_instance_id and nodeHistory.node_status in (1, 2)
     LEFT JOIN dosm_model_node_base_info FOR SYSTEM_TIME AS OF workOrder.proc_time nodeBaseInfo   on workOrder.model_definition_id = nodeBaseInfo.model_definition_id  and nodeBaseInfo.active_node_id = nodeHistory.active_node_id
     LEFT JOIN dosm_work_order_form_data FOR SYSTEM_TIME AS OF workOrder.proc_time nodeFormData on nodeFormData.work_order_id=workOrder.id
     LEFT JOIN (
        select
            distinct handlerUser.process_instance_id as process_instance_id,
            handlerUser.user_id as userId,
            su.name as current_handle_user
        from dosm_work_order_instance wd
        LEFT JOIN dosm_work_order_node_comment FOR SYSTEM_TIME AS OF wd.proc_time handlerUser ON wd.process_instance_id  = handlerUser.process_instance_id
        LEFT JOIN sys_user FOR SYSTEM_TIME AS OF wd.proc_time su on su.id = CAST(handlerUser.user_id as BIGINT)
        where handlerUser.flow_time=0
     ) nm on nm.process_instance_id = workOrder.process_instance_id
) wt
      LEFT JOIN LATERAL TABLE(impFlattenFormDataAndHandlerInfo(handlerFormData(wt.form_data ,wt.model_definition_id),wt.process_instance_id)) AS
    tb(submit_time,handle_group,handle_user,handle_user_id_h,handle_user_id_a,
            applicant,approvalopinion,bizType,checkopinion,checkresult,checkresults,defectordemandID,
            defectsource,defectstatus,defecttype,eABHbYMDgVJvrQM,OPPOcloudmainclass,OPPOcloudsubclass,PMapproval,
            problemstate,product,productLeader,productLine,productmanager,projectlist,rootcauseanalysis,
            sCddFfHQgEbqvzQ,solvedOverdueTime,source,systemascription,systemLevel,systemname,systemnumber,
            plancompletetime,solvedOverdueTime_t,problemstatus) on true;

