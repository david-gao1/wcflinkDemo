package com.gao.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Description 日志请求处理
 * @Author lianggao
 * @Date 2022/1/7 上午10:16
 */
@RestController
@Slf4j  //lombok 产生logger注解
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @RequestMapping("test")
    public String test1() {
        System.out.println("success");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr) {

        //打印数据
//        System.out.println(jsonStr);

        //将数据落盘
//        log.debug(jsonStr);
        log.info(jsonStr);
//        log.warn(jsonStr);
//        log.error(jsonStr);
//        log.trace(jsonStr);

        //将数据写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}
