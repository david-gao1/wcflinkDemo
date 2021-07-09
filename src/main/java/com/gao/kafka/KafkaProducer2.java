package com.gao.kafka;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/5/14 11:24 上午
 * @Version 1.0
 */
public class KafkaProducer2 {
    public static void main(String[] args) throws InterruptedException {
        //参数配置：
        Properties props = new Properties();
        //server列表
        props.put("bootstrap.servers", "localhost:9092");
        //key，value支持序列化
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);


        String topic2 = "kafka_source2";


        while (true) {
            //将对象转为json数据
            String jsonString = JSONObject.toJSONString(getSimpleTypeJson(), SerializerFeature.WriteMapNullValue);
            //生成数据
            ProducerRecord<String, String> infos = new ProducerRecord<String, String>(topic2, jsonString);
            System.out.println(jsonString);

            //发送数据
            kafkaProducer.send(infos);
            Thread.sleep(5000);
        }

    }

    /**
     * 嵌套格式的对象json
     *
     * @return
     */
    public static JSONObject getSimpleTypeJson() {
        JSONObject jsonObjectResult = new JSONObject();
        Random random1 = new Random();
        int i = random1.nextInt();
        jsonObjectResult.put("id", 1);
        jsonObjectResult.put("name", "张三");
        jsonObjectResult.put("randomNum", i);
        return jsonObjectResult;
    }

}
