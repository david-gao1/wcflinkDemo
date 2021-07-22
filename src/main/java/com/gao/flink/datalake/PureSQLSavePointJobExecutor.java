package com.gao.flink.datalake;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class PureSQLSavePointJobExecutor {
    public static Logger log = LoggerFactory.getLogger(PureSQLSavePointJobExecutor.class);

    public static void main(String[] args) {
        log.info("[main] args:{}", Arrays.toString(args));
        String[] strings = new String[]{"-dataflowId", "10367",
                "-driverClass", "com.mysql.cj.jdbc.Driver",
                "-userName", "root",
                "-password", "11111111"};
        System.out.println("[main] args:{}" + Arrays.toString(strings));
        ParameterTool jbParams = ParameterTool.fromArgs(strings);
        log.info("[jbParams] jbParams:{}", jbParams.get("driverClass"));
        System.out.println("[main] args:{}" + jbParams.get("userName"));
    }
}