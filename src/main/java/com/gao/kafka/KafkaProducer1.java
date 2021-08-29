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
public class KafkaProducer1 {
    public static void main(String[] args) throws InterruptedException {
        //参数配置：
        Properties props = new Properties();
        //server列表
        //props.put("bootstrap.servers", "localhost:9092");
        //10.0.12.159:18108,10.0.12.160:18108,10.0.12.161:18108
        props.put("bootstrap.servers", "localhost:9092");
        //key，value支持序列化
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        String topic1 = "source-1";
        int age = 100;
        while (true) {
            JSONObject stuff = new JSONObject();
            stuff.put("id", age);
            stuff.put("deptname", "bigdata");
            stuff.put("emp_id", age + 1);

            //将对象转为json数据
            String jsonString = JSONObject.toJSONString(stuff, SerializerFeature.WriteMapNullValue);
            //生成数据
            ProducerRecord<String, String> infos = new ProducerRecord<String, String>(topic1, jsonString);
            System.out.println(jsonString);
            //发送数据
            kafkaProducer.send(infos);
            Thread.sleep(5000);
            age++;
        }

    }


    /**
     * @return
     */
    public static JSONObject getPgObejct() {
        JSONObject stuff = new JSONObject();
        int age = (int) (Math.random() * 10) + 1;
        stuff.put("id", age);
        stuff.put("sku_id", "sku_002");
        stuff.put("total_amount", "600");
        stuff.put("create_time", "2020-06-11 13:00:00");
        return stuff;
    }

    /**
     * 嵌套格式的对象json
     */
    public static JSONObject getZhangSanJson() {
        //JSONObject jsonObjectResult = new JSONObject();//用于存储数据
        long timeInMillis = Calendar.getInstance().getTimeInMillis();

        JSONObject stuff = new JSONObject();
        //int i = new Random().nextInt();
        stuff.put("d_id", 1);
        JSONObject depart = new JSONObject();
        depart.put("d_id", 2);
        depart.put("d_name", "技术一部");
        stuff.put("department", depart);
        return stuff;
    }


    /**
     * 嵌套格式的对象json
     */
    public static JSONObject getidAndName() {
        JSONObject stuff = new JSONObject();
        int age = (int) (Math.random() * 10) + 1;
        stuff.put("id", 4);
        stuff.put("sku_id", "sku_002");
        stuff.put("total_amount", "600");
        stuff.put("create_time", "2020-06-11 13:00:00");
        return stuff;
    }


    //  `ID`  INTEGER,
    //  `BUSINESS_TYPE`  STRING,
    //  `OCCUR_DATE`  STRING,
    //  `CUSTOMER_ID`  INTEGER,
    //  `BUSINESS_TYPE`  INTEGER,
    //  `CREATE_DATE`  DATE,
    //  `UPDATE_DATE`  DATE
    public static JSONObject getCDC() {
        JSONObject stuff = new JSONObject();
        int age = (int) (Math.random() * 10) + 1;
        stuff.put("ID", age);
        stuff.put("OCCUR_DATE", new Date());
        stuff.put("CUSTOMER_ID", (int) (Math.random() * 10) + 1 + (int) (Math.random() * 10) + 1);
        stuff.put("BUSINESS_TYPE", (int) (Math.random() * 10) + 1 + (int) (Math.random() * 10) + 2);
        stuff.put("BUSINESS_SUM", (int) (Math.random() * 10) + 1 + (int) (Math.random() * 10) + 2);
        stuff.put("CREATE_DATE", new Date());
        stuff.put("UPDATE_DATE", new Date());
        return stuff;
    }


    /**
     * 嵌套格式的对象json
     */
    public static JSONObject getObjectJson() {
        //JSONObject jsonObjectResult = new JSONObject();//用于存储数据
        long timeInMillis = Calendar.getInstance().getTimeInMillis();

        JSONObject stuff = new JSONObject();
        //int i = new Random().nextInt();
        stuff.put("id", 1);
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
        stuff.put("personalMessage", personalMessage);

        return stuff;
    }
}
