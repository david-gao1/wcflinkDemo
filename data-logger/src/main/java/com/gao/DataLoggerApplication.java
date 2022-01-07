package com.gao;

import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Description 启动类
 * @Author lianggao
 * @Date 2022/1/7 上午10:15
 * @Version 1.0
 */
@SpringBootApplication
public class DataLoggerApplication {
    static Logger logger = Logger.getLogger(DataLoggerApplication.class);

    public static void main(String[] args) {

        logger.info("start...............");
        SpringApplication.run(DataLoggerApplication.class, args);
    }
}
