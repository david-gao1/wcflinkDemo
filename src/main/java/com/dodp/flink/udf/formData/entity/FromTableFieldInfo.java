package com.dodp.flink.udf.formData.entity;

import lombok.Data;

import java.util.Map;

/**
 * FROM_TABLE 表格中字段信息
 */
@Data
public class FromTableFieldInfo{
    /**
     * 字段key
     */
    private String fieldKey;
    /**
     * 字段类型
     */
    private String componentType;
    /**
     * 字段标题
     */
    private String title;

    /**
     * 如果为SELECT类型，保存key‘value -> label
     */
    private Map<String,String> enums;

}