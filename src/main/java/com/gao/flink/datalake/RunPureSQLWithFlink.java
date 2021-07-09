package com.gao.flink.datalake;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import static com.gao.flink.datalake.RunOther.getPureSqlStatements;

/**
 * @Description TODO
 * @Author roman.gao
 * @Date 2021/6/24 10:38 上午
 */
public class RunPureSQLWithFlink {


    /**
     * 1、创造执行环境
     */
    static StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    static EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    static StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);


    public static void main(String[] args) throws IOException {
        // obtain query configuration from TableEnvironment
        TableConfig tConfig = streamTableEnvironment.getConfig();
        // set query parameters
        tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24));

        //2、获取sql数组
        String[] sqlStatements = getPureSqlStatements();

        //3、sql分类
        Map<String, List<String>> sqlStatementMap = getSqlStatementMap(sqlStatements);

        //4、执行flink sql
        String flinkJobId = executeSQLs(sqlStatementMap);

        System.out.println("执行成功，flinkId 是：" + flinkJobId);


    }

    /**
     * 对sql数组进行分类存放
     * DDL（CREATE ALTER DROP）
     * DML（INSERT、UPDATE、DELETE）
     * DQL（SELECT）（不支持ing）
     *
     * @param sqlStatements
     */
    private static Map<String, List<String>> getSqlStatementMap(String[] sqlStatements) {
        Map<String, List<String>> sqlTypeAndSqlStatementMap = new HashMap<>();
        List<String> ddlList = new ArrayList<>();
        List<String> dmlList = new ArrayList<>();
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
                ddlList.add(sqlStatement);
            }
        }
        sqlTypeAndSqlStatementMap.put("DDL", ddlList);
        sqlTypeAndSqlStatementMap.put("DML", dmlList);
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
        //streamTableEnvironment.executeSql("create temporary function substrtest as  `com.gao.flink.datalake.SubstringFuncation`");
        streamTableEnvironment.executeSql(
                String.format("create temporary function substrtest as '%s'",
                        "com.gao.flink.datalake.SubstringFuncation"));
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
        TableResult executeResult = statementSet.execute();
        String jobId = executeResult.getJobClient().get().getJobID().toString();

        return jobId;
    }

}