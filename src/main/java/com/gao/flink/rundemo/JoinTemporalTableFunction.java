package com.gao.flink.rundemo;

import com.gao.flink.udf.TimeStampTo13;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

/**
 * @Description 维表join使用table function
 * @Author lianggao
 * @Date 2021/11/3 下午3:45
 * @Version 1.0
 */
public class JoinTemporalTableFunction {
    /**
     * 1、创造执行环境
     */
    public static StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    public static EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    public static StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);


    public static void main(String[] args) throws Exception {

        tEnv.createTemporarySystemFunction("toMills", TimeStampTo13.class);


        //cdc表
        tEnv.executeSql(
                "CREATE TABLE products (\n" +
                        "    stu_id INT,\n" +
                        "     user_action_time AS PROCTIME(),\n" +
                        "     PRIMARY KEY (stu_id) NOT ENFORCED\n" +
                        "  ) WITH (\n" +
                        "    'connector' = 'mysql-cdc',\n" +
                        "    'hostname' = 'localhost',\n" +
                        "    'port' = '3306',\n" +
                        "    'username' = 'root',\n" +
                        "    'password' = '11111111',\n" +
                        "    'database-name' = 'dataflow_test',\n" +
                        "    'table-name' = 'example1'\n" +
                        "  )\n"
        );

        //cdc表
        tEnv.executeSql(
                "CREATE TABLE products_sink (\n" +
                        "    stu_id INT,\n" +
                        "     time13 bigint,\n" +
                        "     PRIMARY KEY (stu_id) NOT ENFORCED\n" +
                        "  ) WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://localhost:3306/dataflow_test',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '11111111',\n" +
                        "  'table-name' = 'example1',\n" +
                        "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        "  'lookup.cache.max-rows' = '10',\n" +
                        "  'lookup.cache.ttl' = '10MINUTE'"+
                "  )\n"
        );


        tEnv.executeSql( "insert into products_sink " +
                "select stu_id,toMills(user_action_time) from products").print();

        streamExecutionEnvironment.execute("joinDemo");

    }

}
