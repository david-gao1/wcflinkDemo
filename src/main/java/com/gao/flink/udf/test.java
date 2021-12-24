package com.gao.flink.udf;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {

    public static void main(String[] args) throws ParseException {

        //时间戳转为日期格式
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); //24小时制
        //日期格式转时间戳
        String dataTime = "2021-01-01T00:00:00.280";
        Date date1 = simpleDateFormat.parse(dataTime);
        long time1 = date1.getTime();
        System.out.println("日期格式转时间戳：" + time1);
    }
}