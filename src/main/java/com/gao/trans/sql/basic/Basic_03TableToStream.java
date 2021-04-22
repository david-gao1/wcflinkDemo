package com.gao.trans.sql.basic;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 将table转换成 DataStrea
 */
public class Basic_03TableToStream {
    public static void main(String[] args) throws Exception {
        //1、获取env，tableEnv
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings ssSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, ssSettings);

        //2、创建输入表
        tableEnv.executeSql(
                "create table myTable(id int,name string) \n" +
                        "with ('connector.type'='filesystem', \n" +
                        "'connector.path'='/Users/lianggao/MyWorkSpace/006data',\n" +
                        "'format.type'='csv')"
        );

        //3、获取table
        Table table = tableEnv.from("myTable");

        //4、转换为流
        //a、如果只有新增(追加)操作，可以使用toAppendStream
        DataStream<Row> appStream = tableEnv.toAppendStream(table, Row.class);
        appStream.map(new MapFunction<Row, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(Row row)
                    throws Exception {
                int id = Integer.parseInt(row.getField(0).toString());
                String name = row.getField(1).toString();
                return new Tuple2<Integer, String>(id, name);
            }
        }).print();

        //b、如果有增加操作，还有删除操作，则使用toRetractStream
        DataStream<Tuple2<Boolean, Row>> retStream = tableEnv.toRetractStream(table, Row.class);
        retStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple3<Boolean, Integer, String>>() {
            @Override
            public Tuple3<Boolean, Integer, String> map(Tuple2<Boolean, Row> tup) {
                /**
                 * A true {@link Boolean} flag indicates an add message, a false flag indicates a retract message.
                 * flag符号位：true：是添加数据；false：是除添加数据外，其他修改数据的方式
                 */
                Boolean flag = tup.f0;
                int id = Integer.parseInt(tup.f1.getField(0).toString());
                String name = tup.f1.getField(1).toString();
                return new Tuple3<Boolean, Integer, String>(flag, id, name);
            }
        }).print();

        env.execute("TableToDataStreamJava"); //env用于提交job
    }
}

