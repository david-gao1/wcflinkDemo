package com.dodp.flink.wide.important;

import com.dodp.flink.AbstractFlinkSQLJobBuilder;
import com.dodp.flink.udf.AddVersionFunction;
import com.dodp.flink.udf.ClickHouseTransformFunction;
import com.dodp.flink.udf.TimeStampTo13;
import com.dodp.flink.udf.formData.fun.FormDataHandledFunction;
import com.dodp.flink.udf.formData.fun.ImportantFormDataFieldsFlattenExtFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName DodiImportEventTableWrapApp.java
 * @Description
 * @createTime 2021年12月25日 11:09:00
 */
@Slf4j
public class DodiImportEventTableWrapApp extends AbstractFlinkSQLJobBuilder {

    public static void main(String[] args) {
        if (!CollectionUtils.sizeIsEmpty(args)) {
            SQL_FILE_PATH = args[0];
            FLINK_ENV_PARALLELISM = Integer.valueOf(args[1]);
            CHECKPOINT_INTERVAL = Long.valueOf(args[2]);
            CHECKPOINT_PATH = args[3];
            IDLE_STATE_RETENTION = Long.valueOf(args[4]);
        }
        try {
            DodiImportEventTableWrapApp app = new DodiImportEventTableWrapApp();
            app.setSqlFilePath(SQL_FILE_PATH);
            app.executeJob();
        } catch (Exception e) {
            log.error("--执行任务【dodi重大事件宽表构建】失败，错误信息为： {}", e);
            e.printStackTrace();
        }
    }

    @Override
    public void registerUserDefinitionFunction() {
        // 注册form_data数据处理的udf
        streamTableEnvironment.createTemporarySystemFunction("handlerFormData", new FormDataHandledFunction());

        // 注册form_data展平冗余的udf
        streamTableEnvironment.createTemporaryFunction("impFlattenFormData", new ImportantFormDataFieldsFlattenExtFunction());

        // 注册添加version的udf
        streamTableEnvironment.createTemporaryFunction("addVersion", new AddVersionFunction());

        // 注册模拟clickhouse中的transform函数的udf
        streamTableEnvironment.createTemporaryFunction("ck_transform", new ClickHouseTransformFunction());

        //处理时间转为13位的毫秒值
        streamTableEnvironment.createTemporarySystemFunction("toMills", TimeStampTo13.class);
    }
}
