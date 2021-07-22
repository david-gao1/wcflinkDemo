package com.gao.flink.datalake.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class FlinkMain
{
    public static void main(String[] args) throws Exception
    {
        // 1,执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 3,添加资源
        ArrayList<Integer> strings = new ArrayList<>();
        strings.add(2);
        strings.add(3);
        strings.add(4);
        DataStreamSource<Integer> dataStreamSource = env.fromCollection(strings);

        // 4,添加到流,去执行接收到的数据进行入库
        dataStreamSource.addSink(new SinkOracle());

        // 5,执行工作,定义一个工作名称
        env.execute(" flink oracle");
    }
}