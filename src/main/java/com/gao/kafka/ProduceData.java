package com.gao.kafka;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/5/14 11:24 上午
 * @Version 1.0
 */
public class ProduceData {
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
        int flag = 0;
        Random random = new Random();

        String topic1 = "topic_gao-0518";

        String topic2 = "topic_gao-0521-nest";


        while (true) {
            //将对象转为json数据
            String jsonString = JSONObject.toJSONString(getObjectJson(), SerializerFeature.WriteMapNullValue);
            //生成数据
            ProducerRecord<String, String> infos = new ProducerRecord<String, String>(topic2, jsonString);
            System.out.println(jsonString);

            //发送数据
            kafkaProducer.send(infos);
            flag++;
            Thread.sleep(5000);
        }

    }

    /**
     * 嵌套格式的对象json
     */
    public static JSONObject getObjectJson() {
        //JSONObject jsonObjectResult = new JSONObject();//用于存储数据
        long timeInMillis = Calendar.getInstance().getTimeInMillis();

        JSONObject stuff = new JSONObject();
        int i = new Random().nextInt();
        stuff.put("d_id", i);
        stuff.put("name", "张三");
        JSONObject depart = new JSONObject();
        depart.put("d_id", timeInMillis);
        depart.put("d_name", "技术一部");
        stuff.put("department", depart);
        //jsonObjectResult.put("data", stuff);

        //row.array
        JSONObject messageObject = new JSONObject();
        JSONArray message = new JSONArray();

        message.add(timeInMillis);
        message.add(timeInMillis + 1);
        message.add(timeInMillis + 2);
        messageObject.put("message", message);
        stuff.put("messageObject", messageObject);

        //array
        JSONArray personalMessage = new JSONArray();
        personalMessage.add(timeInMillis);
        personalMessage.add(timeInMillis + 2);
        personalMessage.add(timeInMillis + 3);
        stuff.put("personalMessage", personalMessage);


        return stuff;
    }


    /**
     * 嵌套格式的对象json
     *
     * @return
     */
    public static JSONObject getSimpleTypeJson() {
        JSONObject jsonObjectResult = new JSONObject();//用于存储数据
        Random random1 = new Random();
        int i = random1.nextInt();

        jsonObjectResult.put("id", i);

        return jsonObjectResult;
    }

}
