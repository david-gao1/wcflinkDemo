package com.dodp.flink.udf.formData.entity;

import java.util.HashSet;
import java.util.Set;
/**
 * Dosm定义的工单提交表格中字段的17种类型
 */
public enum FieldValueType {
    DATE("DATE"),
    INPUT("INPUT"),
    SELECT("SELECT"),
    RADIO("RADIO"),
    RICH_TEXT("RICH_TEXT"),
    TEXTAREA("TEXTAREA"),
    MEMBER("MEMBER"),
    UPLOAD("UPLOAD"),
    MULTI_SELECT("MULTI_SELECT"),
    EVALUATE_STAR("EVALUATE_STAR"),
    CHECKBOX("CHECKBOX"),
    TABLE_FORM("TABLE_FORM"),
    NUMBER("NUMBER"),
    SELECT_MANY("SELECT_MANY"),
    GROUP("GROUP"),
    TIPS("TIPS"),
    CONFIGURATION_ITEM("CONFIGURATION_ITEM");

    private String type;

    FieldValueType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    /**
     * 不需要额外处理的字段
     */
    public static Set<String> NO_HANDLER_TYPES = new HashSet<>();

    /**
     * 判断是否是不需要额外处理的字段
     */
    public static boolean isNoHandleType(String fieldValueType){
        if(NO_HANDLER_TYPES.size() == 0){
            NO_HANDLER_TYPES.add(INPUT.type);
            NO_HANDLER_TYPES.add(RICH_TEXT.type);
            NO_HANDLER_TYPES.add(TEXTAREA.type);
            NO_HANDLER_TYPES.add(UPLOAD.type);
            NO_HANDLER_TYPES.add(NUMBER.type);
            NO_HANDLER_TYPES.add(TIPS.type);
            NO_HANDLER_TYPES.add(CONFIGURATION_ITEM.type);
        }
        return NO_HANDLER_TYPES.contains(fieldValueType);
    }

    /**
     * 需要映射处理的字段
     */
    public static Set<String> MAPPING_HANDLER_TYPES = new HashSet<>();

    /**
     * 判断是否需要映射处理的字段
     */
    public static boolean isMappingHandleType(String fieldValueType){
        if(MAPPING_HANDLER_TYPES.size() == 0){
            MAPPING_HANDLER_TYPES.add(SELECT.type);
            MAPPING_HANDLER_TYPES.add(RADIO.type);
            MAPPING_HANDLER_TYPES.add(MULTI_SELECT.type);
            MAPPING_HANDLER_TYPES.add(CHECKBOX.type);
            MAPPING_HANDLER_TYPES.add(SELECT_MANY.type);
            MAPPING_HANDLER_TYPES.add(GROUP.type);
        }
        return MAPPING_HANDLER_TYPES.contains(fieldValueType);
    }
}
