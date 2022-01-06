package com.dodp.flink.udf.formData.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * 工单自定义form-data表单字段
 */
@Data
public class OrderFieldModelInfo{
    /**
     * 模型id+版本号
     */
    private String modelDefinitionId;
    private String fieldCode;

    /**
     * 与form-data中key对应
     */
    private String fieldKey;

    /**
     * 字段中文名称
     * -- dodi中不需要
     */
    private String fieldName;

    /**
     * 字段值类型
     */
    private String fieldValueType;

    /**
     * 字段属性信息
     */
    private JSONObject fieldInfo;
}