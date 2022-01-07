package com.gao.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
@Api(tags = "日志请求处理")
@RestController
//@Slf4j  //lombok注解：产生logger
public class LoggerController {

    public static Logger log = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @ApiOperation(value = "日志落盘并发送到kafka中")
    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr) {

        //日志打印的方式将数据落盘
        log.info(jsonStr);

        //将数据写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}
