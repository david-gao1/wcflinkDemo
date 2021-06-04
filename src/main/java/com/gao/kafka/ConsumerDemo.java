package com.gao.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 需求：Java代码实现消费者代码
 */
public class ConsumerDemo {

    public static void main(String[] args) {

        Properties prop = new Properties();
        //指定kafka的broker地址
        prop.put("bootstrap.servers", "localhost:9092");
        //指定key-value数据的序列化格式
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());


        //指定消费者组
        prop.put("group.id", "con-1");

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        Collection<String> topics = new ArrayList<String>();
        topics.add("topic_gao-1");
        //订阅指定的topic
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println(consumerRecord);
            }
        }

    }

}
