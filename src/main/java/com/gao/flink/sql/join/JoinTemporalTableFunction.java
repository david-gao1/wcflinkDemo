package com.gao.flink.sql.join;

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
        //创建维表
        tEnv.executeSql(" create table `weibiao` " +
                "( `user_id` INTEGER  ,`age` INTEGER   ," +
                " diversion_proc_time as  proctime() ) with " +
                "('connector'='jdbc','url'='jdbc:mysql://localhost:3306/dataflow_test'," +
                "'username'= 'root','password' = '11111111'," +
                "'table-name' = 'w','driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'lookup.cache.max-rows' = '100000','lookup.cache.ttl' = '10MINUTE')\n");

        //sink 表
        tEnv.executeSql(" create table `sinkTale` " +
                "( gender string ,`stringtime` bigint  ,`user_id` INTEGER   ," +
                " diversion_proc_time as  proctime() ) with " +
                "('connector'='kafka-0.11','topic'='source-2'," +
                "'properties.bootstrap.servers'= 'localhost:9092'," +
                "'properties.max.poll.records'= '5000','properties.group.id' = ''," +
                "'format' = 'json','json.ignore-parse-errors' = 'true','scan.startup.mode' = 'latest-offset')");

        //创建流表
        tEnv.executeSql("\n" +
                "create table `flowTable` " +
                "( `gender` STRING,`stringtime` String,`user_id` INTEGER," +
                "cloud_wise_proc_time  as  proctime() ) with " +
                "('connector'='kafka-0.11','topic'='source-1'," +
                "'properties.bootstrap.servers'= 'localhost:9092'," +
                "'properties.max.poll.records'= '5000','properties.group.id' = ''," +
                "'format' = 'json','json.ignore-parse-errors' = 'true','scan.startup.mode' = 'latest-offset')");

        //过滤维表并创建为
        // 创建和注册时态表函数
        // 指定 "r_proctime" 为时间属性，指定 "r_currency" 为主键
        Table ftable = tEnv.sqlQuery("select * from weibiao where user_id in (1,2)");
        tEnv.createTemporaryView("ftable", ftable);


        TemporalTableFunction ftableTemporalTableFunction = ftable.createTemporalTableFunction(
                ExpressionParser.parseExpression("diversion_proc_time"),
                ExpressionParser.parseExpression("user_id"));
        tEnv.createTemporarySystemFunction("ftableFunction", ftableTemporalTableFunction);

        Table table = tEnv.sqlQuery("\n" +
                "select f.gender as gender ,f.stringtime as stringtime,r.user_id as user_id,r.age as age from flowTable as f ,\n" +
                " LATERAL TABLE (ftableFunction(f.cloud_wise_proc_time)) as r where r.user_id=f.user_id");

        DataStream resultDs = tEnv.toAppendStream(table, Row.class);
        resultDs.print();
        streamExecutionEnvironment.execute("joinDemo");

//        tEnv.createTemporaryView("joinTable", table);
//        StatementSet statementSet = tEnv.createStatementSet();
//        statementSet.addInsertSql("insert into sinkTale select * from joinTable");
//
//
//        TableResult executeResult = statementSet.execute();
//        String jobId = executeResult.getJobClient().get().getJobID().toString();
//        System.out.println(jobId);

    }

}
