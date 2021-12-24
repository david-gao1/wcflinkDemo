package com.gao.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

// 定义函数逻辑
public class SubstringFunction extends ScalarFunction {
    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, end);
    }
}