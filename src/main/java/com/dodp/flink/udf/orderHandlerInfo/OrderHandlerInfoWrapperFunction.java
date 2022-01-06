package com.dodp.flink.udf.orderHandlerInfo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static com.dodp.flink.udf.conts.Constants.*;
import static com.dodp.flink.udf.formData.utils.DataHandlerUtils.getValueFromDataJSON;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName OrderHandlerInfoWrapperFunction.java
 * @Description  组装问题管理 order_handler_info+form_data 数据
 * @createTime 2021年12月28日 10:19:00
 */
@Slf4j
public class OrderHandlerInfoWrapperFunction extends TableFunction<Row> {

    /**
     * dosm｜douc 的MySQL连接器
     */
    Connection mysqlConn;

    /**
     * dosm｜douc  sql执行器
     */
    Statement mySqlStatement;

    /**
     * 开启资源
     * @param context
     * @throws Exception
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        Class.forName(MYSQL_DRIVER_NAME);
        mysqlConn = DriverManager.getConnection(DOSM_MYSQL_URL,MYSQL_USER_NAME,MYSQL_PASSWORD);
        mySqlStatement = mysqlConn.createStatement();

    }

    /**
     * 关闭资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if(null != mySqlStatement){
            mySqlStatement.close();
        }
        if(null != mysqlConn){
            mysqlConn.close();
        }
    }


    @DataTypeHint("ROW<submit_time STRING,handle_group STRING,handle_user STRING,handle_user_id_h STRING,handle_user_id_a STRING," +
            "applicant STRING,approvalopinion STRING,bizType STRING,checkopinion STRING,checkresult STRING,checkresults STRING,defectordemandID STRING," +
            "defectsource STRING,defectstatus STRING,defecttype STRING,eABHbYMDgVJvrQM STRING,OPPOcloudmainclass STRING,OPPOcloudsubclass STRING,PMapproval STRING," +
            "problemstate STRING,product STRING,productLeader STRING,productLine STRING,productmanager STRING,projectlist STRING,rootcauseanalysis STRING," +
            "sCddFfHQgEbqvzQ STRING,solvedOverdueTime STRING,source STRING,systemascription STRING,systemLevel STRING,systemname STRING,systemnumber STRING," +
            "plancompletetime STRING,solvedOverdueTime_t STRING,problemstatus STRING>")
    public void eval(String formData,Integer process_instance_id){
        JSONObject finalFormData = JSONObject.parseObject(formData);

        int extraSize = 0;
        String nullVal = "";
        List<Row> rows = new ArrayList<>();


        Map<String,String> handlerInfoMap = Maps.newHashMap();
        handleInfoDataWrapper(process_instance_id,handlerInfoMap);
        String submit_time = handlerInfoMap.getOrDefault("submit_time","");
        String handle_group = handlerInfoMap.getOrDefault("handle_group","");
        String handle_user = handlerInfoMap.getOrDefault("handle_user","");
        String handle_user_id_h = handlerInfoMap.getOrDefault("handle_user_id_h","");
        String handle_user_id_a = handlerInfoMap.getOrDefault("handle_user_id_a","");

        String applicant = getValueFromDataJSON(finalFormData,"applicant");
        String approvalopinion = getValueFromDataJSON(finalFormData,"approvalopinion");
        String bizType = getValueFromDataJSON(finalFormData,"bizType");
        String checkopinion = getValueFromDataJSON(finalFormData,"checkopinion");
        String checkresult = getValueFromDataJSON(finalFormData,"checkresult");
        String checkresults = getValueFromDataJSON(finalFormData,"checkresults");
        String defectordemandID = getValueFromDataJSON(finalFormData,"defectordemandID");
        String defectsource = getValueFromDataJSON(finalFormData,"defectsource");
        String defectstatus = getValueFromDataJSON(finalFormData,"defectstatus");
        String defecttype = getValueFromDataJSON(finalFormData,"defecttype");
        String eABHbYMDgVJvrQM = getValueFromDataJSON(finalFormData,"eABHbYMDgVJvrQM");
        String OPPOcloudmainclass = getValueFromDataJSON(finalFormData,"OPPOcloudmainclass");
        String OPPOcloudsubclass = getValueFromDataJSON(finalFormData,"OPPOcloudsubclass");
        String PMapproval = getValueFromDataJSON(finalFormData,"PMapproval");
        String problemstate = getValueFromDataJSON(finalFormData,"problemstate");
        String product = getValueFromDataJSON(finalFormData,"product");
        String productLeader = getValueFromDataJSON(finalFormData,"productLeader");
        String productLine = getValueFromDataJSON(finalFormData,"productLine");
        String productmanager = getValueFromDataJSON(finalFormData,"productmanager");
        String projectlist = getValueFromDataJSON(finalFormData,"projectlist");
        String rootcauseanalysis = getValueFromDataJSON(finalFormData,"rootcauseanalysis");
        String sCddFfHQgEbqvzQ = getValueFromDataJSON(finalFormData,"sCddFfHQgEbqvzQ");
        String solvedOverdueTime = getValueFromDataJSON(finalFormData,"solvedOverdueTime");
        String source = getValueFromDataJSON(finalFormData,"source");
        String systemascription = getValueFromDataJSON(finalFormData,"systemascription");
        String systemLevel = getValueFromDataJSON(finalFormData,"systemLevel");
        String systemname = getValueFromDataJSON(finalFormData,"systemname");
        String systemnumber = getValueFromDataJSON(finalFormData,"systemnumber");

        Set<Map.Entry<String, String>> entries = TABLE_DATA_CHILD_MAP.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            String key = entry.getKey();
            Object o = finalFormData.get(key);
            if (o instanceof JSONArray) {
                JSONArray jsonArray = finalFormData.getJSONArray(key);
                if (jsonArray != null && jsonArray.size() > 0) {
                    for (Object obj : jsonArray) {
                        JSONObject jsonObject = (JSONObject) obj;
                        switch (key) {
                            case FROM_TABLE_IMPROVEMENT_MEASURE:
                                collect(Row.of(submit_time,handle_group,handle_user,handle_user_id_h,handle_user_id_a,
                                        applicant,approvalopinion,bizType,checkopinion,checkresult,checkresults,defectordemandID,defectsource,defectstatus,defecttype,eABHbYMDgVJvrQM,OPPOcloudmainclass,OPPOcloudsubclass,PMapproval,problemstate,product,productLeader,productLine,productmanager,projectlist,rootcauseanalysis,sCddFfHQgEbqvzQ,solvedOverdueTime,source,systemascription,systemLevel,systemname,systemnumber,
                                        getValueFromDataJSON("plancompletetime",jsonObject),getValueFromDataJSON("solvedOverdueTime",jsonObject),getValueFromDataJSON("problemstatus",jsonObject)
                                ));
                                break;
                        }
                        extraSize++;
                    }
                }
            }else{
                String typeName = null;
                try {
                    typeName = o.getClass().getTypeName();
                } catch (Exception e) {
                    log.error("------error: {}",e.getLocalizedMessage());
                }

                log.debug(typeName);
            }
        }
        // 应对当前数据中没有对应的需要拆分的字表数据时，插入最基础的数据
        if (extraSize == 0) {
            collect(Row.of(submit_time,handle_group,handle_user,handle_user_id_h,handle_user_id_a,
                    applicant,approvalopinion,bizType,checkopinion,checkresult,checkresults,defectordemandID,defectsource,defectstatus,defecttype,eABHbYMDgVJvrQM,OPPOcloudmainclass,OPPOcloudsubclass,PMapproval,problemstate,product,productLeader,productLine,productmanager,projectlist,rootcauseanalysis,sCddFfHQgEbqvzQ,solvedOverdueTime,source,systemascription,systemLevel,systemname,systemnumber,
                    nullVal,nullVal,nullVal)
            );
        }

    }

    /**
     * 组装order_handler_info数据
     * -- 字段拉平规则
     * SUBMIT_TIME      问题处理    order_handler_info ->> '$."问题处理"."SUBMIT_TIME"'
     * HANDLE_GROUP     问题处理    order_handler_info ->> '$."问题处理"."HANDLE_GROUP"'
     * HANDLE_USER      问题处理    order_handler_info ->> '$."问题处理"."HANDLE_USER"'
     * HANDLE_USER_ID   问题处理    order_handler_info ->> '$."问题处理"."HANDLE_USER_ID"'
     * HANDLE_USER_ID   问题分析    order_handler_info ->> '$."问题分析"."HANDLE_USER_ID"'
     *
     * -- SQL 查询结果
     * {"问题审批": {"HANDLE_GROUP": "Amanda的用户组","HANDLE_USER": "amanda2","HANDLE_USER_ID": "23",    "SUBMIT_TIME":"-"}}
     * {"问题处理": {"HANDLE_GROUP": "Amanda的用户组","HANDLE_USER": "amanda2","HANDLE_USER_ID": "23",    "SUBMIT_TIME":"2021-04-22 04:10:54"}}
     * {"问题创建": {"HANDLE_GROUP": "Amanda的用户组","HANDLE_USER": "amanda2","HANDLE_USER_ID": "23",    "SUBMIT_TIME":"2021-04-22 04:08:57"}}
     *
     */
    private void handleInfoDataWrapper(Integer process_instance_id,Map<String,String> handleInfoMaps ) {
        String querySql =  String.format(QUERY_ORDER_HANDLER_INFO_SQL,DOSM_DB_NAME,DOSM_DB_NAME,DOUC_DB_NAME,DOUC_DB_NAME,DOUC_DB_NAME,process_instance_id);
       try{
           ResultSet resultSet = mySqlStatement.executeQuery(querySql);
           while (resultSet.next()) {
               String abc = resultSet.getString("abc");
               if(Objects.nonNull(abc)){
                    JSONObject jsonObject = JSONObject.parseObject(abc);
                    if(!jsonObject.isEmpty()){
                        if(jsonObject.containsKey(QUESTION_HANDLER_NAME)){
                            JSONObject json = jsonObject.getJSONObject(QUESTION_HANDLER_NAME);
                            handleInfoMaps.put("submit_time",json.getString("SUBMIT_TIME"));
                            handleInfoMaps.put("handle_group", json.getString("HANDLE_GROUP"));
                            handleInfoMaps.put("handle_user", json.getString("HANDLE_USER"));
                            handleInfoMaps.put("handle_user_id_h", json.getString("HANDLE_USER_ID"));
                        }else if(jsonObject.containsKey(QUESTION_ANALYSIS)){
                            JSONObject json = jsonObject.getJSONObject(QUESTION_HANDLER_NAME);
                            handleInfoMaps.put("handle_user_id_a", json.getString("HANDLE_USER_ID"));
                        }
                    }

               }
           }
       }catch (Exception e){
           log.error(e.getMessage());
       }
    }


    /**
     * 扩展字段数据来源
     * -- key： 扩展字段名称  value： 字段在form-data中位置
     */
    public static final Map<String,String> EXTRA_FIELD_SOURCE_MAPPING = Maps.newHashMap();
    public static final Map<String,String> TABLE_DATA_CHILD_MAP = Maps.newHashMap();
    public static final String FROM_TABLE_IMPROVEMENT_MEASURE= "improvementmeasure";
    public static final String ORDER_HANDLER_INFO = "order_handler_info";
    public static final String QUESTION_HANDLER_NAME = "问题处理";
    public static final String QUESTION_ANALYSIS = "问题分析";

    /**
     * 查询组装order_handler_info数据的SQL
     */
    public static final String QUERY_ORDER_HANDLER_INFO_SQL  = " select" +
            "        ow.process_instance_id as process_instance_id," +
            "        if(nc.data_type=0,'-',nc.updated_time ) as sub," +
            "        concat('{\\\"',nc.active_node_name ,'\\\": {\\\"HANDLE_GROUP\\\": \\\"',g.name ," +
            "        '\\\",\\\"HANDLE_USER\\\": \\\"',su.name ,'\\\",\\\"HANDLE_USER_ID\\\": \\\"',su.id ," +
            "        '\\\",    \\\"SUBMIT_TIME\\\":\\\"',if(nc.data_type=0,'-',nc.updated_time )  ,'\\\"}}') as  abc" +
            "    from  %s.dosm_work_order_instance ow" +
            "    left join %s.dosm_work_order_node_comment nc on ow.process_instance_id = nc.process_instance_id" +
            "    left join %s.sys_user   su on su.id = nc.user_id" +
            "    left join %s.sys_group_user  ug on ug.user_id  = su.id" +
            "    left join %s.sys_group   g on g.id = ug.group_id" +
            "    where  ow.is_del = 0  and ow.is_test = 0 and ow.process_instance_id=%d";

    static {
        EXTRA_FIELD_SOURCE_MAPPING.put("SUBMIT_TIME",ORDER_HANDLER_INFO+".问题处理.SUBMIT_TIME");
        EXTRA_FIELD_SOURCE_MAPPING.put("HANDLE_GROUP",ORDER_HANDLER_INFO+".问题处理.HANDLE_GROUP");
        EXTRA_FIELD_SOURCE_MAPPING.put("HANDLE_USER",ORDER_HANDLER_INFO+".问题处理.HANDLE_USER");
        EXTRA_FIELD_SOURCE_MAPPING.put("HANDLE_USER_ID_H",ORDER_HANDLER_INFO+".问题处理.HANDLE_USER_ID");
        EXTRA_FIELD_SOURCE_MAPPING.put("HANDLE_USER_ID_A",ORDER_HANDLER_INFO+".问题分析.HANDLE_USER_ID");

        EXTRA_FIELD_SOURCE_MAPPING.put("applicant","form_data.applicant");
        EXTRA_FIELD_SOURCE_MAPPING.put("approvalopinion","form_data.approvalopinion");
        EXTRA_FIELD_SOURCE_MAPPING.put("bizType","form_data.bizType");
        EXTRA_FIELD_SOURCE_MAPPING.put("checkopinion","form_data.checkopinion");
        EXTRA_FIELD_SOURCE_MAPPING.put("checkresult","form_data.checkresult");
        EXTRA_FIELD_SOURCE_MAPPING.put("checkresults","form_data.checkresults");
        EXTRA_FIELD_SOURCE_MAPPING.put("defectordemandID","form_data.defectordemandID");
        EXTRA_FIELD_SOURCE_MAPPING.put("defectsource","form_data.defectsource");
        EXTRA_FIELD_SOURCE_MAPPING.put("defectstatus","form_data.defectstatus");
        EXTRA_FIELD_SOURCE_MAPPING.put("defecttype","form_data.defecttype");
        EXTRA_FIELD_SOURCE_MAPPING.put("eABHbYMDgVJvrQM","form_data.eABHbYMDgVJvrQM");
        EXTRA_FIELD_SOURCE_MAPPING.put("OPPOcloudmainclass","form_data.OPPOcloudmainclass");
        EXTRA_FIELD_SOURCE_MAPPING.put("OPPOcloudsubclass","form_data.OPPOcloudsubclass");
        EXTRA_FIELD_SOURCE_MAPPING.put("PMapproval","form_data.PMapproval");
        EXTRA_FIELD_SOURCE_MAPPING.put("problemstate","form_data.problemstate");
        EXTRA_FIELD_SOURCE_MAPPING.put("product","form_data.product");
        EXTRA_FIELD_SOURCE_MAPPING.put("productLeader","form_data.productLeader");
        EXTRA_FIELD_SOURCE_MAPPING.put("productLine","form_data.productLine");
        EXTRA_FIELD_SOURCE_MAPPING.put("productmanager","form_data.productmanager");
        EXTRA_FIELD_SOURCE_MAPPING.put("projectlist","form_data.projectlist");
        EXTRA_FIELD_SOURCE_MAPPING.put("rootcauseanalysis","form_data.rootcauseanalysis");
        EXTRA_FIELD_SOURCE_MAPPING.put("sCddFfHQgEbqvzQ","form_data.sCddFfHQgEbqvzQ");
        EXTRA_FIELD_SOURCE_MAPPING.put("solvedOverdueTime","form_data.solvedOverdueTime");
        EXTRA_FIELD_SOURCE_MAPPING.put("source","form_data.source");
        EXTRA_FIELD_SOURCE_MAPPING.put("systemascription","form_data.systemascription");
        EXTRA_FIELD_SOURCE_MAPPING.put("systemLevel","form_data.systemLevel");
        EXTRA_FIELD_SOURCE_MAPPING.put("systemname","form_data.systemname");
        EXTRA_FIELD_SOURCE_MAPPING.put("systemnumber","form_data.systemnumber");


        EXTRA_FIELD_SOURCE_MAPPING.put("plancompletetime","form_data.improvementmeasure.plancompletetime");
        EXTRA_FIELD_SOURCE_MAPPING.put("solvedOverdueTime_t","form_data.improvementmeasure.solvedOverdueTime");
        EXTRA_FIELD_SOURCE_MAPPING.put("problemstatus","form_data.improvementmeasure.problemstatus");

        TABLE_DATA_CHILD_MAP.put(FROM_TABLE_IMPROVEMENT_MEASURE,"plancompletetime,solvedOverdueTime,problemstatus");
    }
}
