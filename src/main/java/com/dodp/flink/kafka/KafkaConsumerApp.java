package com.dodp.flink.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName KafkaConsumerApp.java
 * @Description
 * @createTime 2021年12月25日 15:04:00
 */
public class KafkaConsumerApp {

    public static void main(String[] args) {
        getMessageFromKafka(new String[]{"question_manager"});
    }

    public static KafkaConsumer<String,String> getKafkaConsumer(){
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "10.0.12.213:18108");
        // 制定consumer group
        props.put("group.id", "dyg");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的反序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //设置offset获取位置为从最早开始(从第一条开始，如果不设置默认从最新开始读取)
        //类似于--from-beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // 定义consumer
        return new KafkaConsumer<>(props);
    }

    public static  void getMessageFromKafka(String[] topics){
        if(topics == null || topics.length==0){
            throw new RuntimeException("topic 不能为空");
        }
        KafkaConsumer<String,String> consumer = getKafkaConsumer();
        //指定topic支持多topic--不支持重复消费消息
        consumer.subscribe(Arrays.asList(topics));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord record : consumerRecords) {
                System.out.println(record.topic()+"------"+record.key()+"---------"+record.value());
                JSONObject jsonObject = JSONObject.parseObject(record.value().toString());
                if(jsonObject.containsKey("order_handler_info") && null != jsonObject.get("order_handler_info")){
                    String order_handler_info = jsonObject.getString("order_handler_info");
                    System.out.println(order_handler_info);
                }
                if(jsonObject.containsKey("event_time") && null != jsonObject.get("event_time")){
                    System.out.println("##########event_time : "+jsonObject.get("event_time"));
                }
                //手动提交
                consumer.commitAsync();
            }
        }
    }
}
