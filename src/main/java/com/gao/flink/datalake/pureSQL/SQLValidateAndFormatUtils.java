package com.gao.flink.datalake.pureSQL;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.gao.flink.datalake.pureSQL.utils.FlinkSQLRunnerBuilder.getPureSqlStatements;
import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;


/**
 * //SqlValidation  ing
 *
 * @Description SQL格式化和校验功能
 * @Author roman.gao
 * @Date 2021/6/24 6:44 下午
 */
public class SQLValidateAndFormatUtils {
    public static void main(String[] args) throws IOException {
        for (String pureSqlStatement : getPureSqlStatements()) {
            //String formatSql = getFormatSql(pureSqlStatement);
            List<String> strings = parseFlinkSql(pureSqlStatement);
            System.out.println("formtStr = ");
            System.out.println(strings);
        }
    }

    /**
     * sql的格式化功能
     * 先进行检验，然后再进行格式化
     *
     * @param sql
     * @return
     */
    public static String getFormatSql(String sql) {
        //Map<Boolean, String> isCheckRightAndFormattedSQLsMap = new HashMap<>();
        List<SQLStatement> statementList;
        String formattedSQLs;
        try {
            SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcConstants.OCEANBASE_DRIVER, true);
            parser.setKeepComments(true);
            statementList = parser.parseStatementList();
        } catch (ParserException e) {
            throw new RuntimeException("sql 格式有误，请重新检查sql：" + e.getMessage());
        }
        return SQLUtils.toSQLString(statementList, JdbcConstants.MYSQL, null, null);
    }

    /**
     * 解析&校验 Flink SQL语句
     *
     * @param sql 一整段字符串sql
     * @return sql语句list
     */
    public static List<String> parseFlinkSql(String sql) {
        List<String> sqlList = new ArrayList<>();
        if (sql != null && !sql.isEmpty()) {
            try {
                SqlParser parser = SqlParser.create(sql, SqlParser.configBuilder()
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setQuoting(BACK_TICK)
                        .setUnquotedCasing(Casing.TO_LOWER)   //字段名统一转化为小写
                        .setQuotedCasing(Casing.UNCHANGED)
                        .setConformance(FlinkSqlConformance.DEFAULT)
                        .build()
                );
                List<SqlNode> sqlNodeList = parser.parseStmtList().getList();
                if (sqlNodeList != null && !sqlNodeList.isEmpty()) {
                    for (SqlNode sqlNode : sqlNodeList) {
                        sqlList.add(sqlNode.toString());
                    }
                }
            } catch (Exception e) {
                //todo:哪句sql校验时出现了错误
                throw new RuntimeException("sql 格式有误，请重新检查sql：" + e.getMessage());
            }
        }
        return sqlList;
    }
}
