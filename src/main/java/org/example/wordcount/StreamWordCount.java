package org.example.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 对有状态的流处理的理解
 * 12> (WITH,1)
 * ...
 * 12> (WITH,2)
 * <p>
 * 会在数据处理过程中保存并更新（1->2,说明存储了状态）值
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        // env.setMaxParallelism(32);

        // 从文件中读取数据
        String inputPath = "/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/flinkSQL.demo/0001baisicCDC.sql";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(item -> item.f0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }
}