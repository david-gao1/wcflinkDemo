package com.gao.flink.sql.runPureSQL;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.table.api.TableConfig;

import java.io.IOException;

import static com.gao.flink.sql.runPureSQL.FlinkSQLRunnerBuilder.*;

/**
 * @Description TODO
 * @Author roman.gao
 * @Date 2021/6/24 10:38 上午
 */
public class RunPureSQLWithFlink {

    public static void main(String[] args) throws IOException {
        //1、设置idle time
        // obtain query configuration from TableEnvironment
        TableConfig tConfig = FlinkSQLRunnerBuilder.streamTableEnvironment.getConfig();
        // set query parameters
        tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24));

        String flinkJobId = runFlinkSQL();

        System.out.println("执行成功，flinkId 是：" + flinkJobId);
    }


}