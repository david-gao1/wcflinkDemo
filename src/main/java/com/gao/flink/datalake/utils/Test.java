package com.gao.flink.datalake.utils;


import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description TODO
 * @Author roman.gao
 * @Date 2021/7/15 3:45 下午
 */
public class Test {

    //1、patten
    // static Pattern formatCompile = Pattern.compile("'connector'='(.*?)'");
    static Pattern formatCompile = Pattern.compile("'format'.*=.*'(.*?)'");
    static Pattern hostCompile = Pattern.compile("'hosts'.*=.*'(.*?)'");


    private static void getSQLPropertyInfo(String sql) {
        Matcher matcher = hostCompile.matcher(sql);
        String sqlTurn = sql;
        while (matcher.find()) {
            System.out.println(matcher.groupCount()); //总共有多少组
            String group = matcher.group(1); //组里的第一个 0代表这个组里所有的元素
            if (group.contains(";")) {
                String s1 = group.replaceAll(";", "#" + UUID.randomUUID() + "#");
                sqlTurn = sqlTurn.replaceFirst(group, s1);
                System.out.println(sqlTurn);
            }
        }
    }



    public static void main(String[] args) {
        //
        String sql = "CREATE TABLE `es070818` (\n" +
                "  `d_id`  INTEGER\n" +
                "  `c1`  STRING,\n" +
                "  `c2`  BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = '10.0.9.45:18115;10.0.9.45:18115',\n" +
                "  'index' = 'es_index098',\n" +
                "  'document-type' = 'es_type099',\n" +
                "  'sink.bulk-flush.max-actions' = '10',\n" +
                "  'failure-handler' = 'ignore',\n" +
                "  'format' = 'json'\n" +
                ");\n" +
                "\n" +
                "\n" +
                "CREATE TABLE `es070818` (\n" +
                "  `d_id`  INTEGER\n" +
                "  `c1`  STRING,\n" +
                "  `c2`  BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = '10.0.9.45:181115;12121',\n" +
                "  'index' = 'es_index098',\n" +
                "  'document-type' = 'es_type099',\n" +
                "  'sink.bulk-flush.max-actions' = '10',\n" +
                "  'failure-handler' = 'ignore',\n" +
                "  'format' = 'json'\n" +
                ");";
        getSQLPropertyInfo(sql);


    }
}
