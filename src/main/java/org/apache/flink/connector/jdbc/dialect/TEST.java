package org.apache.flink.connector.jdbc.dialect;

import org.apache.commons.lang3.StringUtils;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;

public class TEST {
    private static transient Connection dbConn;
    public static void main(String[] args) {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(StringUtils.isEmpty("default") ? null : "default");
        properties.setPassword(StringUtils.isEmpty("Rootmaster@777") ? null : "Rootmaster@777");
        BalancedClickhouseDataSource dataSource;
        try {
            if (null == dbConn) {
                dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://10.0.9.44:18100,10.0.9.44:18100/zz", properties);
                dbConn = dataSource.getConnection();

            }
        } catch (SQLException e) {
            throw new RuntimeException("establish clickhouse JDBC connection failed", e);
        }
    }
}
