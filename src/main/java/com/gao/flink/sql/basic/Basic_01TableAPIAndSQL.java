package com.gao.flink.sql.basic;

import org.apache.flink.table.api.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

/***
 *  基础的使用：Flink，使用Table Api 或者是SQL:c
 *      1、创建表
 *      2、查询
 *      3、输出表
 */
public class Basic_01TableAPIAndSQL {
    public static void main(String[] args) {
        //1、useBlinkPlanner 创建env 不能进行转换
        EnvironmentSettings env = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(env);

        //2、executeSql 创建输入表
        tableEnv.executeSql("" +
                "create table myTable(\n" +
                "id VARCHAR,\n" +
                "title VARCHAR,\n" +
                "properties ROW(`foo` VARCHAR)\n" +
                ") with (\n" +
                "'connector.type' = 'filesystem',\n" +
                "'connector.path' = '/Users/lianggao/MyWorkSpace/006data',\n" +
                "'format.type' = 'json'\n" +
                ")");

        //3、查询并输出
        //3.2、使用SQL  打印到输出台上
        Table table = tableEnv.sqlQuery("select * from myTable ");
        ///提交job并打印出来 tableResult包含了执行的结果
        //execute 执行并返回结果
        TableResult tableResult = table.execute();
        tableResult.print();


        //创建输出表
        tableEnv.executeSql("" +
                "create table newTable(\n" +
                "id int,\n" +
                "name string\n" +
                ") with (\n" +
                "'connector.type' = 'filesystem',\n" +
                "'connector.path' = '/Users/lianggao/MyWorkSpace/006data',\n" +
                "'format.type' = 'csv'\n" +
                ")");
        //输出结果到表newTable中
        table.executeInsert("newTable");
    }
}
