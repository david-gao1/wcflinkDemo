package com.gao.flink.datalake;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author: logan.yang
 * @Date: 2021/07/08 上午
 * @Description:
 */

public class SubstringFuncation extends ScalarFunction {

    private Integer begin;
    private Integer end;

    public SubstringFuncation() {
    }

    /**
     * @param str   string类型的字段
     * @param begin 开始截取的位置
     * @param end   结束截取的位置
     * @return 截取后的字符串
     */
    public String eval(String str, Integer begin, Integer end) {
        return str.substring(begin, end);
    }


//    public static void main(String[] args) {
//        StreamTableEnvironment env = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
//        env.createTemporarySystemFunction("subfuncation",SubstringFuncation.class);
//        // env.registerFunction("subfuncation",new SubstringFuncation());
//    }


}
