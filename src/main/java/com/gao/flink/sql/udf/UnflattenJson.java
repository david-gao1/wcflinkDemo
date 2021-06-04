package com.gao.flink.sql.udf;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * @Description json数据的反解析
 * @Author lianggao
 * @Date 2021/5/23 4:15 下午
 * @Version 1.0
 */
public class UnflattenJson {

    public static void main(String[] args) {
        String s = "(ni+hao+ni)/(hao+a)";
        s.replaceAll("("," ");

        String[] s1 = s.split(" ");
        Arrays.stream(s1).forEach(System.out::println);

    }


    /**
     * 解析与反解析
     */
    private static void flattening() {
        String json = "{ \"a\" : { \"b\" : 1, \"c\": null, \"d\": [false, true] }, \"e\": \"f\", \"g\":2.3 }";
        String jsonStr = JsonFlattener.flatten(json);
        System.out.println(jsonStr);
        String unflatten = JsonUnflattener.unflatten(jsonStr);
        System.out.println(unflatten);
    }


}
