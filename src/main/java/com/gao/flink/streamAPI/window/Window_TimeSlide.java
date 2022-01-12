package com.gao.flink.streamAPI.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * wordCount slide window
 */
public class Window_TimeSlide {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据
        /**
         * nc -lk 9999 开启
         */
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        //3.压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                });

        //4.按照单词分组并聚合
        //窗口：长度是10，每4秒输出一个窗口
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = wordToOneDS
                .keyBy(data -> data.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(4)))
                .sum(1);

        //7.输出打印
        sumResult.print();

        //8.执行任务
        env.execute();

    }
}
