package com.gao.trans.sql.basic;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 将DataStream转换成表
 * //TODO：通过pojo直接创建表或view视图
 */
public class Basic_02StreamToTable {
    public static void main(String[] args) {
        //获取StreamTableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings ssSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, ssSettings);

        //创建DataStream
        DataStreamSource<Tuple2<Integer, String>> stream = env.fromElements(
                new Tuple2(1, "jack"),
                new Tuple2(2, "tom"),
                new Tuple2(3, "mick")
        );


        //第一种：使用sql
        // 将DataStream转换为view视图
        tableEnv.createTemporaryView("myTable", stream, $("id"), $("name"));
        tableEnv.sqlQuery("select * from myTable where id > 1").execute().print();

        //第二种：使用Table Api
        // 将DataStream转换为table对象
        Table table = tableEnv.fromDataStream(stream, $("id"), $("name"));
        table.select($("id"), $("name"))
                //.filter($("id").isGreater(1))  多种api的灵活使用
                .where($("id").isGreater(1))
                .execute()
                .print();
    }
}
