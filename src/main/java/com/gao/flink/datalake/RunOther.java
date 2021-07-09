package com.gao.flink.datalake;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Description 执行sql
 * @Author roman.gao
 * @Date 2021/6/24 9:47 上午
 */
public class RunOther {
    public static void main(String[] args) throws IOException {
        //getPureSqlStatements();
//        String sqlStatement="0 1 2   3 4";
//
//        List<String> collect = Stream.of(sqlStatement.split(" ")).filter(StringUtils::isNotEmpty).collect(Collectors.toList());
//
//        System.out.println("============");
//
//        for (String s1 : collect) {
//            System.out.println(s1);
//        }
        String sqlStatement="0 1 2   3 4";
        String[] split = sqlStatement.split(";");
        List<String> strings = Arrays.asList(split);
        System.out.println(strings.toString());



    }


    /**
     * 获取sql语句
     *
     * @return 数组的形式返回每条sql
     * @throws IOException
     */
    public static String[] getPureSqlStatements() throws IOException {
        //FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/001basic.SQL");
        //FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/002window.sql");
        //FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/003windowMutiSql.sql");
        //FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/004kafkajoinSql-fault.sql");
        //FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/0041kafkajoin.sql");
        FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/005UDF.sql");
        BufferedReader br = new BufferedReader(fr);
        String line = "";
        StringBuilder stringBuilder = new StringBuilder();
        while ((line = br.readLine()) != null) {
            stringBuilder.append(line);
        }
        br.close();
        fr.close();
        String[] split = stringBuilder.toString().split(";");
        //System.out.println(Arrays.toString(split));
        return split;
    }


}
