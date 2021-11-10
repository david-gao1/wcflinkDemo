package com.gao.flink.datalake.sqltoStream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/11/1 下午3:59
 * @Version 1.0
 */
public class FlinksqlTOStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE `send1` (\n" +
                "  `c1`  STRING,\n" +
                "  `c2`  BIGINT,\n" +
                "  `cloud_wise_proc_time` AS `proctime`()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka-0.11',\n" +
                "  'topic' = 'send072001',\n" +
                "  'properties.bootstrap.servers' = '10.0.9.44:18108,10.0.9.45:18108',\n" +
                "  'properties.max.poll.records' = '5000',\n" +
                "  'properties.group.id' = '',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
                ")");

        //sql转换为流
        if (false) {
            return;

        }
        //send1 类似于中间表的处理 send1最后作为源表数据
        String getFilteringSql = "getFlatteningSql()";
        Table table = tableEnv.sqlQuery(getFilteringSql);

        TableSchema schema = table.getSchema();


        DataStream<Row> kafkaStream = tableEnv.toRetractStream(table, Row.class).map(f -> f.f1).uid("getUID");
        //kafkaStream 设置为











    }
}
