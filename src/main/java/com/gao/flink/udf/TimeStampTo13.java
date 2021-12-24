package com.gao.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 转为13位的毫秒值
 */
public class TimeStampTo13 extends ScalarFunction {
    public long eval(Timestamp timestampFiled) {
        return timestampFiled.getTime();
    }
}
