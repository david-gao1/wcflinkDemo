package com.flink.cdc;

import com.dodp.flink.udf.AddVersionFunction;
import com.dodp.flink.udf.ClickHouseTransformFunction;
import com.dodp.flink.udf.formData.fun.FormDataHandledFunction;
import com.dodp.flink.udf.formData.fun.ImportantFormDataFieldsFlattenExtFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static com.dodp.flink.udf.conts.Constants.*;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName FormDataUdfUseApp.java
 * @Description 测试form_data数据重组和拉平冗余udf
 * @createTime 2021年12月24日 09:47:00
 */
public class FormDataUdfUseDemo {

    /**
     * 工单form-data字段所在表
     * -- 不必将所有字段取出来，只取需要的字段
     * -- PRIMARY KEY (id) NOT ENFORCED
     */
    public static String DOSM_WORK_ORDER_FORM_DATA_SQL =
            "CREATE TABLE dosm_work_order_form_data (" +
                    "  id STRING ," +
                    "  work_order_id STRING," +
                    "  plan_start_time STRING," +
                    "  plan_end_time STRING," +
                    "  work_order_form_data STRING," +
                    "  PRIMARY KEY (id) NOT ENFORCED" +
                    ") WITH ("+
                    "'connector' ='jdbc'," +
                    "'url' = 'jdbc:mysql://10.0.10.186:18103/test?useUnicode=true&characterEncoding=utf-8'," +
                    "'driver' = 'com.mysql.cj.jdbc.Driver', " +
                    "'table-name' = 'dosm_work_order_form_data', " +
                    "'username' = 'Rootmaster'," +
                    "'password' = 'Rootmaster@777'," +
                    "'scan.fetch-size' = '3')";


    public static String DOSM_WORK_ORDER_INSTANCE_SQL=
            "CREATE TABLE dosm_work_order_instance ("+
                    "id STRING    ,"+
                    " primary_catalog_id STRING    ,"+
                    " catalog_id STRING    ,"+
                    " primary_catalog_id STRING,"+
                    " model_definition_key STRING    ,"+
                    " model_definition_id STRING   ,"+
                    " created_user_id STRING,"+
                    " created_time STRING,"+
                    " vip_level STRING,"+
                    " title STRING,"+
                    " label STRING,"+
                    " ticket_desc STRING,"+
                    " process_instance_id INT,"+
                    " is_del INT,"+
                    " is_test INT,"+
                    "  PRIMARY KEY (id) NOT ENFORCED" +
                    ") WITH ('connector' ='jdbc'," +
                    "'url' = 'jdbc:mysql://10.0.10.186:18103/dosm_activiti?useUnicode=true&characterEncoding=utf-8'," +
                    "'driver' = 'com.mysql.cj.jdbc.Driver', " +
                    "'table-name' = 'dosm_work_order_instance', " +
                    "'username' = 'Rootmaster'," +
                    "'password' = 'Rootmaster@777'," +
                    "'scan.fetch-size' = '3')";
    /**
     * 用户 字段字典数据表
     */
    public static String SYS_USER_SQL =
            "CREATE TABLE sys_user (" +
                    "  id DECIMAL,"+
                    "  name STRING ," +
                    "  PRIMARY KEY (id) NOT ENFORCED " +
                    ") WITH ('connector' ='jdbc'," +
                    "'url' = 'jdbc:mysql://10.0.10.186:18103/douc_1?useUnicode=true&characterEncoding=utf-8'," +
                    "'driver' = 'com.mysql.cj.jdbc.Driver', " +
                    "'table-name' = 'sys_user', " +
                    "'username' = 'Rootmaster'," +
                    "'password' = 'Rootmaster@777'," +
                    "'scan.fetch-size' = '3')";


    public static String QUERY_SQL = "select " +
            " workOrder.id as id ," +
            " workOrder.model_definition_key as model_definition_key ," +
            " workOrder.model_definition_id as model_definition_id ," +
            " workOrder.process_instance_id as process_instance_id ," +
            " workOrder.created_user_id as created_user_id ," +
            " suser.name as created_user_name," +
            " workOrder.catalog_id  as catalog_id  ," +
            " workOrder.primary_catalog_id as primary_catalog_id ," +
            " workOrder.vip_level as vip_level ," +
            " workOrder.title as title," +
            " workOrder.label as label," +
            " workOrder.ticket_desc as ticket_desc" +
//            " formatDateTime(workOrder.created_time,'%F %T') as  created_time," +
//            " formatDateTime(workOrder.updated_time,'%F %T') as  updated_time," +
//            " if(workOrder.order_status = 'FINISHED',formatDateTime(workOrder.updated_time,'%F %T'),'') as  finish_time " +
            " from dosm_work_order_instance workOrder  " +
            " left join sys_user suser on suser.id = CAST(workOrder.created_user_id as INT)" +
            "where workOrder.is_del = 0 and workOrder.is_test = 0 and workOrder.id ='d778e235d7b74d5a846520a6a9b25547'";
    
    public static String OTHER_SQL = "select   " +
            "  wo.id as woid,  " +
            "  wo.model_definition_id as model_definition_id, "+
//            "  nodeFormData.work_order_form_data as form_data,  " +
            "  handlerFormData(nodeFormData.work_order_form_data ,wo.model_definition_id  ) as form_data_2"+
            "  nodeFormData.plan_start_time as plan_start_time,  " +
            "  nodeFormData.plan_end_time as plan_end_time   " +
            "   FROM  dosm_work_order_instance wo    " +
            "   left join  dosm_work_order_form_data nodeFormData on nodeFormData.work_order_id = wo.id " +
            "   where wo.id ='d778e235d7b74d5a846520a6a9b25547'";


    public static String FLATTERN_FORM_DATA_SQL = "select   " +
            "  '7efa4d1de47841be900b098c16f3932c:219' as model_definition_id, "+
//            "  handlerFormData(work_order_form_data ,'7efa4d1de47841be900b098c16f3932c:219') as form_data_2, "+
            "  plan_start_time as plan_start_time,  " +
            "  plan_end_time as plan_end_time   " +
            "  FROM  dosm_work_order_form_data ,    " +
            "  LATERAL TABLE(impFlattenFormData(handlerFormData(work_order_form_data ,'7efa4d1de47841be900b098c16f3932c:219'))) as " +
            " t(problemstate,breakstarttime,liabilitydepartment,liabilitygroup,reasonclass,reasonsubclass,CHorRL, systemname,systemgrade,belongdepartment,systemnumber,affectedinformation,improvemeasurelevel,improvemeasuretype,turnproblem,responsegroup,responseperson,plancompletetime,responsedepartment,infrastructureownership,onelevelservicedirectory,twolevelservicedirectory,unavailabletime)";
//            "   where wo.work_order_id ='d778e235d7b74d5a846520a6a9b25547'";
    
    public static String FLATTERN_FORM_DATA_SQL_2= "select   " +
            " m.model_definition_id, " +
            " m.plan_start_time, " +
            " m.plan_end_time ," +
            IMPORTANT_EVENT_FORM_DATA_FLTTERN_FIELDS +
            " ,addVersion() as version " +
            " ,'BE_PROCESSED' as order_status_origin " +
            " ,ck_transform('BE_PROCESSED',array['FINISHED','PROCESSING','BE_PROCESSED','CLOSED'],array['已完成','处理中','待领取','已关闭'],'其他') as order_status " +
            "from( " +
            " select  " +
            "  'd366dad215274e1ab4a325e0722b0196:37' as model_definition_id, " +
            "  plan_start_time as plan_start_time,  " +
            "     plan_end_time as plan_end_time, " +
            "     handlerFormData(work_order_form_data ,'d366dad215274e1ab4a325e0722b0196:37') as form_data_2" +
            "     from dosm_work_order_form_data " +
            ")m" +
            " LEFT JOIN LATERAL TABLE(impFlattenFormData(form_data_2)) AS tb("+IMPORTANT_EVENT_FORM_DATA_FLTTERN_FIELDS+") ON TRUE";
//            ",LATERAL TABLE(impFlattenFormData(form_data_2)) as "+IMPORTANT_EVENT_FORM_DATA_FLTTERN_FIELDS;


    public static void main(String[] args) {
        //1、useBlinkPlanner 创建env 不能进行转换
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, environmentSettings);


        //2、executeSql 创建输入表
        streamTableEnvironment.executeSql(DOSM_WORK_ORDER_FORM_DATA_SQL);
        streamTableEnvironment.executeSql(DOSM_WORK_ORDER_INSTANCE_SQL);
        streamTableEnvironment.executeSql(SYS_USER_SQL);

        // 测试form_data数据处理
        streamTableEnvironment.createTemporarySystemFunction("handlerFormData",new FormDataHandledFunction());
        // 测试form_data展平冗余
        streamTableEnvironment.createTemporaryFunction("impFlattenFormData",new ImportantFormDataFieldsFlattenExtFunction());
        // 测试添加version
        streamTableEnvironment.createTemporaryFunction("addVersion",new AddVersionFunction());
        // 测试模拟clickhouse中的transform函数
        streamTableEnvironment.createTemporaryFunction("ck_transform",new ClickHouseTransformFunction());

        System.out.println(FLATTERN_FORM_DATA_SQL_2);

        //3、查询并输出
        // 3.2、使用SQL  打印到输出台上
//        Table formDataTable = streamTableEnvironment.sqlQuery(OTHER_SQL);
        Table formDataTable = streamTableEnvironment.sqlQuery(FLATTERN_FORM_DATA_SQL_2);
            /// 提交job并打印出来 tableResult包含了执行的结果

        // execute 执行并返回结果
        TableResult formDataTableResult = formDataTable.execute();
        CloseableIterator<Row> collect = formDataTableResult.collect();
        while (collect.hasNext()) {
            Row next = collect.next();
            int arity = next.getArity();
            StringBuilder sub = new StringBuilder();
            for (int i = 0; i < arity; i++) {
                sub.append(next.getField(i)).append(" , ");
            }
            System.out.println("-----fieldDefinitionDataTable data : "+ sub.toString());
        }
        formDataTableResult.print();

    }
}
