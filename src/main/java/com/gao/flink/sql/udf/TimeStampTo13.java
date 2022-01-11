package com.gao.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * 转为13位的毫秒值
 */
public class TimeStampTo13 extends ScalarFunction {
    public long eval(Timestamp timestampFiled) {
        return timestampFiled.getTime();
    }
}
