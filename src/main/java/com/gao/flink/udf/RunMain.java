package com.gao.flink.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;


public class RunMain {
    /**
     * 1、创造执行环境
     */
    static StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    static EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    static StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);

    public static void main(String[] args) {
        streamTableEnvironment.executeSql(
                String.format("create temporary function subStrTest as '%s'",
                        "com.gao.flink.datalake.udf.SubstringFuncation"));
        String[] strings = streamTableEnvironment.listUserDefinedFunctions();
        System.out.println("[listUserDefinedFunctions] is{}" + Arrays.toString(strings));

        streamTableEnvironment.executeSql("create table MyTable ( `id` INTEGER NOT NULL  )");
        // 在 SQL 里调用注册好的函数
        streamTableEnvironment.sqlQuery("SELECT substrtest(id, 5, 12) FROM MyTable");


    }
}
