package com.gao.flink.sql.join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/11/4 下午3:00
 * @Version 1.0
 */
public class JoinExplain {

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    static StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    public static void main(String[] args) {

        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("table.optimizer.source.predicate-pushdown-enabled", String.valueOf(true));

        //创建维表
        tEnv.executeSql(" create table `weibiao` " +
                "( `user_id` INTEGER  ,`age` INTEGER ) with " +
                "('connector'='jdbc','url'='jdbc:mysql://localhost:3306/dataflow_test'," +
                "'username'= 'root','password' = '11111111'," +
                "'table-name' = 'w','driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'lookup.cache.max-rows' = '100000','lookup.cache.ttl' = '10MINUTE')\n");

        //创建流表
        tEnv.executeSql("\n" +
                "create table `flowTable` " +
                "( `gender` STRING,`stringtime` String,`user_id` INTEGER," +
                "cloud_wise_proc_time  as  proctime() ) with " +
                "('connector'='kafka-0.11','topic'='source-1'," +
                "'properties.bootstrap.servers'= 'localhost:9092'," +
                "'properties.max.poll.records'= '5000','properties.group.id' = ''," +
                "'format' = 'json','json.ignore-parse-errors' = 'true','scan.startup.mode' = 'latest-offset')");


        String s = tEnv.explainSql("SELECT\n" +
                "  *\n" +
                "FROM\n" +
                "  flowTable AS o\n" +
                "  JOIN weibiao FOR SYSTEM_TIME AS OF o.cloud_wise_proc_time AS r\n" +
                "  ON r.user_id = o.user_id where r.user_id>1 and r.age>1");

        System.out.println(s);
        TableResult tableResult = tEnv.executeSql(
                "SELECT\n" +
                        "  *\n" +
                        "FROM\n" +
                        "  flowTable AS o \n" +
                        "  JOIN weibiao FOR SYSTEM_TIME AS OF o.cloud_wise_proc_time AS r\n" +
                        "  ON r.user_id = o.user_id where r.user_id>1 and r.age>1");
        tableResult.print();
        //FilterableTableSource
        /**
         * == Abstract Syntax Tree ==
         * LogicalProject(gender=[$0], stringtime=[$1], user_id=[$2], cloud_wise_proc_time=[$3], user_id0=[$4], age=[$5])
         * +- LogicalFilter(condition=[AND(>($4, 1), >($5, 1))])
         *    +- LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{2, 3}])
         *       :- LogicalProject(gender=[$0], stringtime=[$1], user_id=[$2], cloud_wise_proc_time=[PROCTIME()])
         *       :  +- LogicalTableScan(table=[[default_catalog, default_database, flowTable]])
         *       +- LogicalFilter(condition=[=($0, $cor0.user_id)])
         *          +- LogicalSnapshot(period=[$cor0.cloud_wise_proc_time])
         *             +- LogicalTableScan(table=[[default_catalog, default_database, weibiao]])
         *
         * == Optimized Logical Plan ==
         * Calc(select=[gender, stringtime, user_id, PROCTIME_MATERIALIZE(cloud_wise_proc_time) AS cloud_wise_proc_time, user_id0, age])
         * +- LookupJoin(table=[default_catalog.default_database.weibiao], joinType=[InnerJoin], async=[false], lookup=[user_id=user_id], where=[AND(>(user_id, 1), >(age, 1))], select=[gender, stringtime, user_id, cloud_wise_proc_time, user_id, age])
         *    +- Calc(select=[gender, stringtime, user_id, PROCTIME() AS cloud_wise_proc_time])
         *       +- TableSourceScan(table=[[default_catalog, default_database, flowTable]], fields=[gender, stringtime, user_id])
         *
         * == Physical Execution Plan ==
         * Stage 1 : Data Source
         * 	content : Source: TableSourceScan(table=[[default_catalog, default_database, flowTable]], fields=[gender, stringtime, user_id])
         *
         * 	Stage 2 : Operator
         * 		content : Calc(select=[gender, stringtime, user_id, () AS cloud_wise_proc_time])
         * 		ship_strategy : FORWARD
         *
         * 		Stage 3 : Operator
         * 			content : LookupJoin(table=[default_catalog.default_database.weibiao], joinType=[InnerJoin], async=[false], lookup=[user_id=user_id], where=[((user_id > 1) AND (age > 1))], select=[gender, stringtime, user_id, cloud_wise_proc_time, user_id, age])
         * 			ship_strategy : FORWARD
         *
         * 			Stage 4 : Operator
         * 				content : Calc(select=[gender, stringtime, user_id, PROCTIME_MATERIALIZE(cloud_wise_proc_time) AS cloud_wise_proc_time, user_id0, age])
         * 				ship_strategy : FORWARD
         */

    }
}
