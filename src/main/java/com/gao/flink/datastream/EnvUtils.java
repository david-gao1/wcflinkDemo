package com.gao.flink.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/5/11 9:20 下午
 * @Version 1.0
 */
public class EnvUtils {
    /**
     * 001：怎样获取执行环境
     */
    public static StreamExecutionEnvironment getDataStreamEnv() {
        /**
         * 会根据实际情况去创建不同的执行环境，所以下面两种就不需要使用了
         *  1、本地运行
         *  2、创建集群环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return env;

//        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
//        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment
//                .createRemoteEnvironment();
//        //String host, int port, String... jarFiles

    }

    /**
     * 002:通过运行环境的不同的方法获取流数据
     */
    public DataStream<String> getDataStreamFromEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.readTextFile("file:///path/to/file");
        return text;


    }

    /**
     * 003:通过dataStream调用算子可以衍生出新的dataStream
     */
    public void createNewDataStreamBytrans() {
        DataStream<String> stringDataStream = getDataStreamFromEnv();
        DataStream<Integer> integerDataStream = stringDataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String inputString) throws Exception {
                return Integer.valueOf(inputString);
            }
        });
    }

    /**
     * 004：创建sink的几种方式
     * 通过
     */
    public void getSink() {
        //writeAsText(String path)

        //print()
    }

    /**
     * 005：调用env的execute来触发app的执行
     * 根据你的环境：会将app提交到flink集群上去运行或在本地执行
     */
    public void triggerApp() throws Exception {
        StreamExecutionEnvironment dataStreamEnv = getDataStreamEnv();
        /**
         * +1流数据的load或者create
         * +2算子的转换
         *
         */
        dataStreamEnv.execute("11");
    }

    /**
     * 006：异步执行和获取job的执行情况
     *
     */
    void triggerAppWithAsnyc() throws Exception {
        StreamExecutionEnvironment dataStreamEnv = getDataStreamEnv();

        final JobClient jobClient = dataStreamEnv.executeAsync();

        /**
         * 此方法可以观察到flink job的执行情况
         */
       // final JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult(userClassloader).get();

    }


}
