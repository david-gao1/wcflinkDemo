package com.dodp.flink.wide.question;

import com.dodp.flink.AbstractFlinkSQLJobBuilder;
import com.dodp.flink.udf.AddVersionFunction;
import com.dodp.flink.udf.ClickHouseTransformFunction;
import com.dodp.flink.udf.formData.fun.FormDataHandledFunction;
import com.dodp.flink.udf.orderHandlerInfo.OrderHandlerInfoWrapperFunction;
import lombok.extern.slf4j.Slf4j;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName DodiQuestionManagerTableWrapApp.java
 * @Description dodi 问题管理业务宽表
 * @createTime 2021年12月28日 09:40:00
 */
@Slf4j
public class DodiQuestionManagerTableWrapApp extends AbstractFlinkSQLJobBuilder {
    public static final String SQL_FILE_PATH = "flinkSQL/questionManager/question.sql";
    public static void main(String[] args) {
        try {
            DodiQuestionManagerTableWrapApp app = new DodiQuestionManagerTableWrapApp();
            app.setSqlFilePath(SQL_FILE_PATH);
            app.executeJob();
        } catch (Exception e) {
            log.error("--执行任务【dodi问题管理宽表构建】失败，错误信息为： {}", e);
            e.printStackTrace();
        }
    }

    @Override
    public void registerUserDefinitionFunction() {

        // 注册form_data数据处理的udf
        streamTableEnvironment.createTemporarySystemFunction("handlerFormData",new FormDataHandledFunction());

        // 注册添加version的udf
        streamTableEnvironment.createTemporaryFunction("addVersion",new AddVersionFunction());

        // 注册模拟clickhouse中的transform函数的udf
        streamTableEnvironment.createTemporaryFunction("ck_transform",new ClickHouseTransformFunction());

        // 注册处理handler_info+ form_data展品处理

        // 注册form_data数据处理的udf
        streamTableEnvironment.createTemporarySystemFunction("impFlattenFormDataAndHandlerInfo",new OrderHandlerInfoWrapperFunction());
    }
}
