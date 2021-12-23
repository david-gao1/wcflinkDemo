package com.gao.flink.datalake.pureSQL.utils;

import com.gao.flink.datalake.udf.SubstringFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalogUtils;
import org.apache.flink.connector.jdbc.catalog.PostgresCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * @Description TODO
 * @Author roman.gao
 * @Date 2021/8/22 3:27 下午
 */
public class FlinkSQLRunnerBuilder {

    /**
     * 1、创造执行环境
     */
    public static StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    public static EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    public static StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);

    /**
     * runFlinkSQL
     *
     * @return flinkJobId
     * @throws IOException
     */
    public static String runFlinkSQL() throws IOException {
        PostgresCatalog postgresCatalog = getPostgresCatalog();
        streamTableEnvironment.registerCatalog("mypg", postgresCatalog);
        //2、获取sql数组
        String[] sqlStatements = getPureSqlStatements();

        //3、sql分类
        Map<String, List<String>> sqlStatementMap = getSqlStatementMap(sqlStatements);

        //4、执行flink sql
        String flinkJobId = executeSQLs(sqlStatementMap);
        return flinkJobId;
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
        //FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/0042kafkajoin.sql");
        //FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/0041kafkajoin.sql");
//        FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/005UDF.sql");
//        FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/flinkPureSQL/clickhouse/001kafkaToClickhouse.sql");
        FileReader fr = new FileReader("/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/flinkPureSQL/catalog/002pgSinkKafka.SQL");

        //002pgSinkKafka.SQL
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


    /**
     * 对sql数组进行分类存放
     * DDL（CREATE ALTER DROP）
     * DML（INSERT、UPDATE、DELETE）
     * DQL（SELECT）（不支持ing）
     *
     * @param sqlStatements
     */
    public static Map<String, List<String>> getSqlStatementMap(String[] sqlStatements) {
        Map<String, List<String>> sqlTypeAndSqlStatementMap = new HashMap<>();
        List<String> ddlList = new ArrayList<>();
        List<String> dmlList = new ArrayList<>();
        ArrayList<String> selectList = new ArrayList<>();
        List<String> ddlTypes = Arrays.asList("CREATE", "DROP", "ALTER");
        List<String> dmlTypes = Arrays.asList("INSERT", "UPDATE", "DELETE");
        for (String sqlStatement : sqlStatements) {
            String sqlType = Stream.of(sqlStatement.split(" "))
                    .filter(StringUtils::isNotEmpty)
                    .findFirst()
                    .orElse(" ");
            if (ddlTypes.contains(sqlType.toUpperCase())) {
                ddlList.add(sqlStatement);
            } else if (dmlTypes.contains(sqlType.toUpperCase())) {
                dmlList.add(sqlStatement);
            } else {
                selectList.add(sqlStatement);
            }
        }
        sqlTypeAndSqlStatementMap.put("DDL", ddlList);
        sqlTypeAndSqlStatementMap.put("DML", dmlList);
        sqlTypeAndSqlStatementMap.put("SELECT", selectList);
        return sqlTypeAndSqlStatementMap;
    }


    /**
     * 纯sql模式下 执行sql 并提交job
     *
     * @param sqlTypeAndSqlStatementMap
     * @return
     */
    public static String executeSQLs(Map<String, List<String>> sqlTypeAndSqlStatementMap) {
        //streamTableEnvironment.createTemporarySystemFunction("substrtest", new SubstringFuncation());
        //streamTableEnvironment.executeSql("create temporary function substrtest as  `com.gao.flink.datalake.udf.SubstringFuncation`");
        //streamTableEnvironment.createTemporarySystemFunction("substrtest", SubstringFuncation.class);
        streamTableEnvironment.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);
//        streamTableEnvironment.executeSql(
//                String.format("create temporary function substrtest as '%s'",
//                        "com.gao.flink.datalake.udf.SubstringFunction"));
        String[] strings = streamTableEnvironment.listUserDefinedFunctions();
        System.out.println("[listUserDefinedFunctions] is{}" + Arrays.toString(strings));
        StatementSet statementSet = streamTableEnvironment.createStatementSet();
        for (String ddlStatement : sqlTypeAndSqlStatementMap.get("DDL")) {
            System.out.println("[executeSQLs] ddlStatement is {}" + ddlStatement);
            streamTableEnvironment.executeSql(ddlStatement);
        }
        for (String dmlStatement : sqlTypeAndSqlStatementMap.get("DML")) {
            System.out.println("[executeSQLs] dmlStatement is {}" + dmlStatement);
            statementSet.addInsertSql(dmlStatement);
        }
        for (String select : sqlTypeAndSqlStatementMap.get("SELECT")) {
            System.out.println("[executeSQLs] SELECTStatement is {}" + select);
            streamTableEnvironment.sqlQuery(select);
        }
        TableResult executeResult = statementSet.execute();
        String jobId = executeResult.getJobClient().get().getJobID().toString();

        return jobId;
    }


    /**
     * 创建catalog
     *
     * @return
     */
    private static PostgresCatalog getPostgresCatalog() {
        String catalogName = "mypg";//自定义名字？
        String defaultDatabase = "flinktest";
        String username = "postgres";
        String pwd = "11111111";
        //baseUrl要求是不能带有数据库名的
        String baseUrl = "jdbc:postgresql://localhost:5432/";
        PostgresCatalog postgresCatalog = (PostgresCatalog) JdbcCatalogUtils.createCatalog(
                catalogName,
                defaultDatabase,
                username,
                pwd,
                baseUrl);
        return postgresCatalog;
    }
}
