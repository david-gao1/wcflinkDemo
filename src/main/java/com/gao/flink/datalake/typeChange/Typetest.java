package com.gao.flink.datalake.typeChange;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.data.TimestampData;


import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.locks.Lock;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/11/9 下午4:17
 * @Version 1.0
 */
public class Typetest {
    public static void main(String[] args) throws SQLException {
        Date date = new Date();
        System.out.println(date);
        //Lock
    }





    public Object deserialize(Object val) throws SQLException {
        if (val instanceof oracle.sql.TIMESTAMP){
            return TimestampData.fromTimestamp(((oracle.sql.TIMESTAMP) val).timestampValue());
        }else if (val instanceof oracle.sql.TIMESTAMPLTZ){
            return TimestampData.fromTimestamp((oracle.sql.TIMESTAMP.toTimestamp(
                    ((oracle.sql.TIMESTAMPLTZ) val).toBytes())));
            //((oracle.sql.TIMESTAMPLTZ) val).toBytes()
        }else {
            return TimestampData.fromTimestamp(((oracle.sql.TIMESTAMPTZ) val).timestampValue());
        }
    }
}
