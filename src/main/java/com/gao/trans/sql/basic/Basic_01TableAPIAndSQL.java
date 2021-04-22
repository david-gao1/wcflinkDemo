package com.gao.trans.sql.basic;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/***
 *  基础的使用：Flink，使用Table Api 或者是SQL:c
 *      1、创建表
 *      2、查询
 *      3、输出表
 */
public class Basic_01TableAPIAndSQL {
    public static void main(String[] args) {
        //1、获取TableEnvironment
        EnvironmentSettings env = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(env);

        //2、创建
        // 创建输入表
        //推荐使用executeSql
        tableEnv.executeSql("" +
                "create table myTable(\n" +
                "id int,\n" +
                "name string\n" +
                ") with (\n" +
                "'connector.type' = 'filesystem',\n" +
                "'connector.path' = '/Users/lianggao/MyWorkSpace/006data',\n" +
                "'format.type' = 'csv'\n" +
                ")");

        //3、查询并输出
        //3.1、使用Table API
//        tableEnv.from("myTable")
//                .select($("id"), $("name"))
//                .filter($("id").isGreater(1))
//                .execute()
//                .print();
        //3.2、使用SQL
        tableEnv.sqlQuery("select id,name from myTable where id > 1")
                .execute()
                .print();


//？？？？
//        //创建输出表
//        sTableEnv.executeSql("" +
//                "create table newTable(\n" +
//                "id int,\n" +
//                "name string\n" +
//                ") with (\n" +
//                "'connector.type' = 'filesystem',\n" +
//                "'connector.path' = '/Users/lianggao/MyWorkSpace/006data',\n" +
//                "'format.type' = 'csv'\n" +
//                ")");
//        //输出结果到表newTable中
//        result.executeInsert("newTable");
    }
}
