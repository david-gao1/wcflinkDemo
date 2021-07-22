package com.gao.flink.datalake.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description TODO
 * @Author roman.gao
 * @Date 2021/7/20 6:57 下午
 */
public class FLinkMainTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env);
        Configuration conf = ste.getConfig().getConfiguration();
        conf.setString("table.exec.sink.not-null-enforcer", "drop");

        ste.executeSql("CREATE TABLE `send1` (\n" +
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

        ste.executeSql("CREATE TABLE `send2` (\n" +
                "  `c1`  STRING,\n" +
                "  `c2`  BIGINT\n" +
                ") WITH (\n" +
                "  'connector.type' = 'jdbc',\n" +
                "  'connector.url' = 'jdbc:mysql://10.0.9.45:18103/test',\n" +
                "  'connector.username' = 'Rootmaster',\n" +
                "  'connector.password' = 'Rootmaster@777',\n" +
                "  'connector.table' = 'test',\n" +
                "  'connector.driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                "  'connector.scan.auto-commit' = 'true'\n" +
                ")");

        ste.executeSql("INSERT INTO `send2`\n" +
                "(SELECT `c1`,`c2`\n" +
                "FROM `send1`)");
    }
}
