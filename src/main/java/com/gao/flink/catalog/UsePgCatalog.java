package com.gao.flink.catalog;

import com.google.common.collect.Lists;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalogUtils;
import org.apache.flink.connector.jdbc.catalog.PostgresCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Description 使用catalog基础点
 * @Author roman.gao
 * @Date 2021/8/18 10:09 上午
 */
public class UsePgCatalog {
    /**
     * 1、创造执行环境
     */
    public static StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    public static EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    public static StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);


    public static void main(String[] args) throws DatabaseNotExistException, TableNotExistException, IOException {
//        getDatasFromPGCatalog();
        usePgCatalog();
    }

    /**
     * 通过streamTableEnvironment+register catalog 查询表数据
     * <p>
     * 返回的结果
     * 1,IT                                                ,1
     * 2,bigdata                                           ,1
     * 3,front                                             ,1
     * 4,ROK                                               ,2
     */
    private static void getDatasFromPGCatalog() {
        PostgresCatalog postgresCatalog = getPostgresCatalog();
        streamTableEnvironment.registerCatalog(postgresCatalog.getName(), postgresCatalog);
        //streamTableEnvironment.useCatalog(postgresCatalog.getName());

        Table table = streamTableEnvironment.sqlQuery("select * from mypg.flinktest.department");
        ArrayList<Row> rows = Lists.newArrayList(table.execute().collect());
        rows.forEach(System.out::println);
    }

    /**
     * 获取源数据信息
     *
     * @throws DatabaseNotExistException
     * @throws TableNotExistException
     */
    private static void usePgCatalog() throws DatabaseNotExistException, TableNotExistException {
        PostgresCatalog postgresCatalog = getPostgresCatalog();
        //true
        boolean flinktest = postgresCatalog.databaseExists("flinktest");
        //展示所有的数据库[postgres, flinktest]
        List<String> strings = postgresCatalog.listDatabases();
        //返回为空
        CatalogDatabase flinktest1 = postgresCatalog.getDatabase("flinktest");
        //数据库中的表（带有默认的模式）[public.department, public.depatmentout, public.j_t]
        List<String> flinktest2 = postgresCatalog.listTables("flinktest");
        System.out.println("tables is " + flinktest2);

        //参考：展示字段
        //https://blog.csdn.net/zhangjun5965/article/details/107825999
        CatalogBaseTable table = postgresCatalog.getTable(new ObjectPath("flinktest", "department"));
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        for (int i = 0; fieldNames.length > i; i++) {
            System.out.println(fieldNames[i] + "  " + schema.getFieldDataType(fieldNames[i]));
        }
        System.out.println(Arrays.toString(fieldNames));
        System.out.println(flinktest + " " + strings + " " + flinktest1 + " " + flinktest2);
    }

    /**
     * 创建catalog
     *
     * @return
     */
    private static PostgresCatalog getPostgresCatalog() {
        String catalogName = "mypg";
        String defaultDatabase = "flinktest";
        String username = "postgres";
        String pwd = "11111111";
        String baseUrl = "jdbc:postgresql://localhost:5432/";//baseUrl要求是不能带有数据库名的
        PostgresCatalog postgresCatalog = (PostgresCatalog) JdbcCatalogUtils.createCatalog(
                catalogName,
                defaultDatabase,
                username,
                pwd,
                baseUrl);
        return postgresCatalog;
    }


    private static void usePgCatalogs() throws DatabaseNotExistException, TableNotExistException {
        PostgresCatalog postgresCatalog = getPostgresCatalog();
        boolean flinktest = postgresCatalog.databaseExists("flinktest");
        List<String> strings = postgresCatalog.listDatabases();
        CatalogDatabase flinktest1 = postgresCatalog.getDatabase("flinktest");
        List<String> flinktest2 = postgresCatalog.listTables("flinktest");

        CatalogBaseTable table = postgresCatalog.getTable(new ObjectPath("flinktest", "department"));
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        for (int i = 0; fieldNames.length > i; i++) {
            System.out.println(fieldNames[i] + "  " + schema.getFieldDataType(fieldNames[i]));
        }
    }
}
