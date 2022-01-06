package com.dodp.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName AddVersionFunction.java
 * @Description 新增version列数据的udf
 * @createTime 2021年12月26日 16:39:00
 */
public class AddVersionFunction extends ScalarFunction {

    public Long eval(){

        return System.currentTimeMillis();
    }

    public long eval(Timestamp timestampFiled) {
        return timestampFiled.getTime();
    }

}
