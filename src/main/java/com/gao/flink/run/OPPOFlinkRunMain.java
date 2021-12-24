package com.gao.flink.run;

import java.io.IOException;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/12/23 下午2:59
 * @Version 1.0
 */
public class OPPOFlinkRunMain {
    public static void main(String[] args) throws IOException {
        FlinkSQLRunnerBuilder runnerBuilder = new FlinkSQLRunnerBuilder();

        runnerBuilder.runMain();
    }

}
