package com.gao.flink.streamAPI.state;


import com.gao.flink.streamAPI.window.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 温度报警
 * <p>
 * sensor_1,1547718199,35.8
 * sensor_1,1547718199,32.4
 * sensor_1,1547718199,42.4
 * sensor_10,1547718205,52.6
 * sensor_10,1547718205,22.5
 * sensor_7,1547718202,6.7
 * sensor_7,1547718202,9.9
 * sensor_1,1547718207,36.3
 * sensor_7,1547718202,19.9
 * sensor_7,1547718202,30
 */
public class StateTest3_KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个flatmap操作，检测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));

        resultStream.print();

        env.execute();
    }

    // 实现自定义函数类
    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        // 私有属性，温度跳变阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();

            // 如果状态不为null，那么就判断两次温度差值
            if (lastTemp != null) {
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                //温度差值大于阈值就报警（发出数据）
                if (diff >= threshold)
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
            }
            // 更新状态（更新温度值为最近一次，因为要比较最近两次的温度差值）
            lastTempState.update(value.getTemperature());
        }

        /**
         * 关闭上下文清除状态
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
