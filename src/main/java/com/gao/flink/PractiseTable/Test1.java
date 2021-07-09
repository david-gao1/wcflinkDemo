package com.gao.flink.PractiseTable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/5/17 4:21 下午
 * @Version 1.0
 */
public class Test1 {
    public static void main(String[] args) {
//        //创建StreamTableEnvironment
//        StreamExecutionEnvironment ssEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings ssSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        TableEnvironment streamTableEnv = StreamTableEnvironment.create(ssEnv, ssSettings);
//
//
//        //创建table对象
//        Table sqlResult = streamTableEnv.sqlQuery("SELECT ... FROM table1 ... ");


        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd  HH:mm:ss ");
        Date date = new Date(System.currentTimeMillis());
        System.out.println(formatter.format(date));




    }
}
