package com.gao.flink.practise.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 1、Streaming WordCount
 * 实时消费socket生产的内容，对2秒内的时间窗口进行聚合计算
 */
public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception {
        //1、获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2、连接socket获取输入数据
        DataStreamSource<String> text = env.socketTextStream("192.168.133.128", 9001);

        //3、处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = text.flatMap(
                //对于函数式接口可以使用lambda
                new FlatMapFunction<String, String>() {
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                }).map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                }).keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    public String getKey(Tuple2<String, Integer> tup) throws Exception {
                        return tup.f0;
                    }
                })//.keyBy(0)
                .timeWindow(Time.seconds(2))
                .sum(1);

        //4、使用一个线程执行打印操作
        wordCount.print().setParallelism(1);

        //5、执行程序
        env.execute("SocketWindowWordCountJava");

    }
}
