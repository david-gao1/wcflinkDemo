package com.gao.flink.datalake.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkOracle extends RichSinkFunction<Integer>
{
    private Connection connection;
    private PreparedStatement statement;

    // 1,初始化
    @Override
    public void open(Configuration parameters) throws Exception
    {
        super.open(parameters);
        Class.forName("oracle.jdbc.driver.OracleDriver");
        connection = DriverManager.getConnection("jdbc:oracle:thin:@10.0.7.124:1521:orcl", "CDC", "yunzhihui123");
        statement = connection.prepareStatement("INSERT INTO flink_oracle_test VALUES (11)");
    }

    // 2,执行
    @Override
    public void invoke(Integer value, Context context) throws Exception
    {
        System.out.println("value.toString()-------" + value.toString());
        statement.setInt(1, value);
        statement.execute();
    }

    // 3,关闭
    @Override
    public void close() throws Exception
    {
        super.close();
        if (statement != null)
            statement.close();
        if (connection != null)
            connection.close();
    }
}