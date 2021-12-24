package com.gao.flink.run;

import com.gao.flink.udf.SubstringFunction;
import com.gao.flink.udf.TimeStampTo13;
import org.apache.commons.lang3.StringUtils;
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


public class FlinkSQLRunnerBuilder {

    StreamExecutionEnvironment env;
    StreamTableEnvironment streamTableEnvironment;


    public String runMain() throws IOException {
        //1、创建执行环境
        createEnv();
        //2、获取sql
        Map<String, List<String>> sqlStatementMap = getSQLs();
        //3、执行flink sql
        return executeSQLs(sqlStatementMap);
    }

    /**
     * 获取sql数组并对sql进行分类
     *
     * @return
     * @throws IOException
     */
    private Map<String, List<String>> getSQLs() throws IOException {
        String[] sqlStatements = getPureSqlStatements();
        return getSqlStatementMap(sqlStatements);
    }

    /**
     * 创建flink的执行环境，
     * todo：指定ip和端口或者直接getExecutionEnvironment
     */
    private void createEnv() {

//        env = StreamExecutionEnvironment.createRemoteEnvironment(
//               "localhost",
//                8081);

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        streamTableEnvironment = StreamTableEnvironment.create(env, environmentSettings);
    }

    /**
     * 获取sql语句
     *
     * @return 数组的形式返回每条sql
     * @throws IOException
     */
    public static String[] getPureSqlStatements() throws IOException {
        String filePath = "/Users/lianggao/MyWorkSpace/001project/wcflinkDemo/src/main/resources/OPPO/0002dsjJoin.sql";
        FileReader fr = new FileReader(filePath);
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
        List<String> ddlTypes = Collections.singletonList("CREATE");
        List<String> dmlTypes = Collections.singletonList("INSERT");
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
    public String executeSQLs(Map<String, List<String>> sqlTypeAndSqlStatementMap) {

        //todo：全局udf
        //udf
        streamTableEnvironment.createTemporarySystemFunction("toMills", TimeStampTo13.class);
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
}
