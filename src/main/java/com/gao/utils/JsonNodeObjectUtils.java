package com.gao.utils;

import java.util.Arrays;
import java.util.List;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/5/10 3:46 下午
 * @Version 1.0
 */
public class JsonNodeObjectUtils {
    public static void main(String[] args) {
        List<String> basicDataTypes =
                Arrays.asList("STRING", "BOOLEAN", "INTEGER", "BIGINT",
                        "FLOAT", "DOUBLE", "DATE", "TIME", "TIMESTAMP");
        boolean contains = basicDataTypes.contains("string");
        System.out.println(contains);

    }


}
