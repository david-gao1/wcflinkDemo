package com.dodp.flink.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName FlinkUtil.java
 * @Description
 * @createTime 2021年12月26日 17:14:00
 */
@Slf4j
public class FlinkExecuteJobUtil {

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
     * 获取sql语句
     * @param filePath SQL文件路径
     * @return 数组的形式返回每条sql
     * @throws IOException
     */
    public static String[] getPureSqlStatements(String filePath) throws IOException {
        log.info("--从SQL文件【{}】 获取SQL语句",filePath);
        FileReader fr = new FileReader(filePath);
        BufferedReader br = new BufferedReader(fr);
        String line = "";
        StringBuilder stringBuilder = new StringBuilder();
        while ((line = br.readLine()) != null) {
            if("" == line || line.startsWith("--")){
                continue;
            }
            stringBuilder.append(line);
        }
        br.close();
        fr.close();
        String[] split = stringBuilder.toString().split(";");
        log.info("--从SQL文件【{}】 解析SQL语句成功",filePath);
        return split;
    }

    /**
     * 纯sql模式下 执行sql 并提交job
     * @param sqlTypeAndSqlStatementMap   分类的SQL Map集合
     * @param streamTableEnvironment      Flink Table 执行环境
     * @return
     */
    public static String executeSQLs(Map<String, List<String>> sqlTypeAndSqlStatementMap, StreamTableEnvironment streamTableEnvironment) throws Exception{
        String[] strings = streamTableEnvironment.listUserDefinedFunctions();
        log.info("--[listUserDefinedFunctions] is {}" + Arrays.toString(strings));

        StatementSet statementSet = streamTableEnvironment.createStatementSet();
        for (String ddlStatement : sqlTypeAndSqlStatementMap.get("DDL")) {
            log.info("[executeSQLs] ddlStatement is {} " ,ddlStatement);
            streamTableEnvironment.executeSql(ddlStatement);
        }
        for (String dmlStatement : sqlTypeAndSqlStatementMap.get("DML")) {
            log.info("[executeSQLs] dmlStatement is {} ",dmlStatement);
            statementSet.addInsertSql(dmlStatement);
        }
//        for (String select : sqlTypeAndSqlStatementMap.get("SELECT")) {
//            log.info("[executeSQLs] SELECTStatement is {} " , select);
//           //  System.out.println(select);
//            Table table = streamTableEnvironment.sqlQuery(select);
//            TableResult execute = table.execute();
//            CloseableIterator<Row> collect = execute.collect();
//            while (collect.hasNext()) {
//                Row next = collect.next();
//                int arity = next.getArity();
//                StringBuilder sub = new StringBuilder();
//                for (int i = 0; i < arity; i++) {
//                    sub.append(next.getField(i)).append(" , ");
//                }
//                log.info("-----data : {}", sub.toString());
//                System.out.println("-----data : "+ sub.toString());
//            }
//        }
        String jobId = "";
        TableResult executeResult = statementSet.execute();
        jobId = executeResult.getJobClient().get().getJobID().toString();
        return jobId;
    }

}
