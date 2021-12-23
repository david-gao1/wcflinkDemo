package com.gao.flink.datalake.pureSQL;

import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class PureSQLSavePointJobExecutor {
    public static Logger log = LoggerFactory.getLogger(PureSQLSavePointJobExecutor.class);

    public static void main(String[] args) throws SQLException {
        Connection conn1 = DriverManager.getConnection("jdbc:clickhouse://10.0.6.84:18100/", "default", "Rootmaster@777");
//        PreparedStatement ps1 =
//                conn1.prepareStatement("SELECT * FROM system.tables where name = ?;");
//        ps1.setString(1, "t_order_mt");
//        ResultSet rs = ps1.executeQuery();
        PreparedStatement ps =
                conn1.prepareStatement(String.format("SELECT * FROM %s;", "roman.t_order_mt"));
        //set table Name and type(change to flink field type)
        ResultSetMetaData rsmd = ps.getMetaData();
        String[] names = new String[rsmd.getColumnCount()];
        DataType[] types = new DataType[rsmd.getColumnCount()];



//        Optional<UniqueConstraint> primaryKey;
//        String pkName = rs.getString(1);
//        String[] pknames = pkName.split(",");
//
//        List<String> pkFields = Arrays.asList(pknames); // initialize size
//        if (CollectionUtils.isNotEmpty(pkFields)) {
//            primaryKey = Optional.of(UniqueConstraint.primaryKey(pkName, pkFields));
//        } else {
//            primaryKey = Optional.empty();
//        }
//        System.out.println("pkFields" + pkFields);
    }
}