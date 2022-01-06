package com.dodp.flink.udf.conts;

import java.text.SimpleDateFormat;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName Constants.java
 * @Description  公共静态数据
 * @createTime 2021年12月23日 22:23:00
 */
public interface Constants {
    /**
     * form-data 中需要删除的字段名称
     */
    String[] REMOVE_FIELDS = {"title", "urgentLevel", "ticketDesc"};

    /**
     * 查询用户名称的SQL
     */
    String SYS_USER_NAME_SQL = "select id,name from %s.sys_user where id=%d";

    /**
     * DATE类型最终展示的格式化器
     */
    SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * MySQL 驱动类
     */
    String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";

    /**
     * douc数据库地址
     */
    String DOUC_MYSQL_URL = "jdbc:mysql://10.0.10.186:18103/dosm_activiti_oppo?useUnicode=true&characterEncoding=utf-8";

    /**
     * dosm数据库地址
     */
    String DOSM_MYSQL_URL = "jdbc:mysql://10.0.10.186:18103/dosm_activiti?useUnicode=true&characterEncoding=utf-8";

    /**
     * 数据库用户名
     */
    String MYSQL_USER_NAME = "Rootmaster";

    /**
     * 数据库密码
     */
    String MYSQL_PASSWORD = "Rootmaster@777";

    String DOUC_DB_NAME = "douc_oppo";
    String DOSM_DB_NAME = "dosm_activiti_oppo";

    /**
     * 用户表
     */
    String MYSQL_SYS_USER_TABLE = "sys_user";

    /**
     * 工单提交表单数据表
     */
    String MYSQL_DOSM_WORK_ORDER_FORM_DATA_TABLE = "dosm_work_order_form_data";

    /**
     * 工单模型字段类型 字典表
     */
    String MYSQL_DOSM_MODEL_FIELD_DEFINITION_TABLE = "dosm_model_field_definition";

    /**
     * 获取字段数据信息
     */
    String FIELD_INFO_SQL = "select model_definition_id ,field_code ,field_key ,field_name, field_value_type ,field_info  " +
            "from dosm_model_field_definition  where model_definition_id ='%s'";

    /**
     * 重大事件中需要从form_data中拉平的字段
     */
    String IMPORTANT_EVENT_FORM_DATA_FLTTERN_FIELDS = " problemstate,breakstarttime,liabilitydepartment,liabilitygroup,reasonclass,reasonsubclass,CHorRL," +
            "systemname,systemgrade,belongdepartment,systemnumber,affectedinformation," +
            "improvemeasurelevel,improvemeasuretype,turnproblem,responsegroup,responseperson,plancompletetime,responsedepartment," +
            "infrastructureownership,onelevelservicedirectory,twolevelservicedirectory,unavailabletime ";

}
