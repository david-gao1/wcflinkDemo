package com.gao.flink.sql.basic;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 创建TableEnvironment对象
 * Created by xuwei
 */
public class Basic_00CreateTableEnv {
    public void mainTest() {
        /**
         * 1、注意：如果Table API和SQL不需要和DataStream或者DataSet互相转换
         * 则针对stream和batch都可以使用TableEnvironment
         */
        //创建TableEnvironment对象-stream
        EnvironmentSettings sSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment sTableEnv = TableEnvironment.create(sSettings);

        //创建TableEnvironment对象-batch
        EnvironmentSettings bSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bTableEnv = TableEnvironment.create(bSettings);


        /**
         * 2、注意：如果Table API和SQL需要和DataStream或者DataSet互相转换
         * 针对stream需要使用StreamTableEnvironment
         * 针对batch需要使用BatchTableEnvironment
         */
        //创建StreamTableEnvironment
        StreamExecutionEnvironment ssEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings ssSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ssTableEnv = StreamTableEnvironment.create(ssEnv, ssSettings);

        //创建BatchTableEnvironment
        //注意：此时只能使用旧的执行引擎，新的Blink执行引擎不支持和DataSet转换
        ExecutionEnvironment bbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment bbTableEnv = BatchTableEnvironment.create(bbEnv);
    }
}
