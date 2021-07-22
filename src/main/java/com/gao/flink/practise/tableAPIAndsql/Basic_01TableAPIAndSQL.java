package com.gao.flink.practise.tableAPIAndsql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 *  基础的使用：Flink，使用Table Api 或者是SQL:c
 *      1、创建表
 *      2、查询
 *      3、输出表
 */
public class Basic_01TableAPIAndSQL {
    public static void main(String[] args) throws Exception {
        //1、获取TableEnvironment 并设置kafka源
        StreamExecutionEnvironment ssEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings ssSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ssTableEnv = StreamTableEnvironment.create(ssEnv, ssSettings);


        //2、创建
        // 创建输入表
        //推荐使用executeSql
        ssTableEnv.executeSql("" +
                "create table gao_test_052302 ( \n" +
                "`department.d_id` BIGINT NOT NULL, \n" +
                "`department.d_name` STRING NOT NULL, \n" +
                "`messageObject.message[1]` BIGINT NOT NULL, \n" +
                "`messageObject.message[2]` BIGINT NOT NULL, \n" +
                "`messageObject.message[3]` BIGINT NOT NULL, \n" +
                "`name` STRING NOT NULL, \n" +
                "`personalMessage[1]` BIGINT NOT NULL, \n" +
                "`personalMessage[2]` BIGINT NOT NULL, \n" +
                "`personalMessage[3]` BIGINT NOT NULL, \n" +
                "`s_id` INTEGER NOT NULL )  \n" +
                "with ('connector'='kafka-0.11', \n" +
                "'topic'='topic_gao-0521-nest', \n" +
                "'properties.bootstrap.servers'= '192.168.2.100:9092', \n" +
                "'properties.group.id' = 'testGroup', \n" +
                "'json.ignore-parse-errors' = 'true', \n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'format' = 'json') "
        );

        //3、查询并输出
        //3.1、使用Table API
//        tableEnv.from("myTable")
//                .select($("id"), $("name"))
//                .filter($("id").isGreater(1))
//                .execute()
//                .print();
        //3.2、使用SQL  打印到输出台上
        //table类似于是dataStream和DataSet
        Table resultTable = ssTableEnv.sqlQuery("select `department.d_id` from gao_test_052302 ");
        //收集当前本地客户端的内容
        TableResult execute = resultTable.execute();
        execute.print();

        ssEnv.execute("121");
    }
}
