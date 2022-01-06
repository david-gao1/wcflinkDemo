package com.dodp.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.dodp.flink.util.FlinkExecuteJobUtil.*;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName AbastrctFlinkSQLBuilder.java
 * @Description
 * @createTime 2021年12月26日 17:28:00
 */
public abstract class AbstractFlinkSQLJobBuilder {

    /**
     * 待提交的sql
     */
    public static String SQL_FILE_PATH;//= "/Users/lianggao/MyWorkSpace/001project/flink-cdc-oppo/src/main/resources/flinkSQL.demo/0001baisicCDC.sql";
    /**
     * job的并行度
     */
    public static Integer FLINK_ENV_PARALLELISM = 1;
    /**
     * checkpoint 生成时间间隔 单位：秒
     */
    public static Long CHECKPOINT_INTERVAL = 30L;
    /**
     * checkpoint 生成路径
     */
    public static String CHECKPOINT_PATH;


    /**
     * 状态空闲过期时间 单位：小时
     */
    public static Long IDLE_STATE_RETENTION = 12L;


    /**
     * Flink 执行环境
     */
    protected StreamExecutionEnvironment env;
    /**
     * Flink 流式表执行环境
     */
    protected StreamTableEnvironment streamTableEnvironment;
    /**
     * 任务相关SQL所在文件
     */
    protected String sqlFilePath;


    /**
     * 提交Flink job 任务
     *
     * @return
     */
    public String executeJob() throws Exception {
        //  1、创建执行环境
        createEnv();
        //  2、获取sql
        Map<String, List<String>> sqlStatementMap = getSqlMap();
        //  3、注册需要使用到的
        registerUserDefinitionFunction();
        //  4、执行flink sql
        return executeSQLs(sqlStatementMap, streamTableEnvironment);
    }


    /**
     * 创建flink的执行环境，因为是提交到flink集群使用getExecutionEnvironment，即flink根据环境情况分配执行资源。
     */
    protected void createEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        streamTableEnvironment = StreamTableEnvironment.create(env, environmentSettings);
        //动态设置flink运行参数
        setEnvOption();
    }

    private void setEnvOption() {
        //设置并行度
        env.setParallelism(FLINK_ENV_PARALLELISM);

        //设置：状态空闲过期时间
        /**
         * 在空闲时间小于最小时间之前，状态永远不会被清除，在空闲之后的某个时间段会被清除。
         * 默认是永远不清理状态。注意：清理状态需要额外的记账开销。默认值是0，这意味着永远不会清理状态。
         *
         * 有状态的操作：聚合：去重,,,；分组
         */
        streamTableEnvironment.getConfig().setIdleStateRetention(Duration.ofHours(IDLE_STATE_RETENTION));


        /**
         * 重启策略:失败时尝试重启3次,每次重启间隔为10秒
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10, TimeUnit.SECONDS))
        );


        /**
         *  设置checkpoint
         */
        //两个checkpoint之间的间隔时间，默认模式：精确一次
        env.enableCheckpointing(CHECKPOINT_INTERVAL * 1000);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //检查点失败容忍次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //默认情况下会加载flink-conf.yaml里的配置信息，此处的设置会覆盖配置文件中的
//        env.setStateBackend(new FsStateBackend("file:///Users/lifuqiang/checkpoint"));


    }

    /**
     * 获取sql数组并对sql进行分类
     *
     * @return
     * @throws IOException
     */
    protected Map<String, List<String>> getSqlMap() throws IOException {
        String[] sqlStatements = getPureSqlStatements(sqlFilePath);
        return getSqlStatementMap(sqlStatements);
    }


    public void setSqlFilePath(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }

    /**
     * 注册自定义UDF
     */
    public abstract void registerUserDefinitionFunction() throws Exception;
}
