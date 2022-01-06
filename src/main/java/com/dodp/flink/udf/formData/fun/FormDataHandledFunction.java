package com.dodp.flink.udf.formData.fun;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dodp.flink.udf.formData.entity.FromTableFieldInfo;
import com.dodp.flink.udf.formData.entity.OrderFieldModelInfo;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static com.dodp.flink.udf.conts.Constants.*;
import static com.dodp.flink.udf.formData.entity.FieldValueType.*;
import static com.dodp.flink.udf.formData.utils.DataHandlerUtils.*;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName FormDataHandledFunction.java
 * @Description 重组form_data数据的UDF
 * @createTime 2021年12月23日 18:07:00
 */
@Slf4j
public class FormDataHandledFunction extends ScalarFunction {

    /**
     * dosm｜douc 的MySQL连接器
     */
    Connection doucConn;
    Connection dosmConn;

    /**
     * dosm｜douc  sql执行器
     */
    Statement doucStatement;
    Statement dosmStatement;

    /**
     * 开启资源
     * @param context
     * @throws Exception
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        Class.forName(MYSQL_DRIVER_NAME);
        dosmConn = DriverManager.getConnection(DOSM_MYSQL_URL,MYSQL_USER_NAME,MYSQL_PASSWORD);
        dosmStatement = dosmConn.createStatement();

        doucConn = DriverManager.getConnection(DOUC_MYSQL_URL,MYSQL_USER_NAME,MYSQL_PASSWORD);
        doucStatement = doucConn.createStatement();
    }

    /**
     * 关闭资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if(null != doucStatement){
            doucStatement.close();
        }
        if(null != dosmStatement){
            dosmStatement.close();
        }
        if(null != doucConn){
            doucConn.close();
        }
        if(null != dosmConn){
            dosmConn.close();
        }
    }

    /**
     * 实现的方法
     * @param originFormData  原始form_data
     * @param modelDefinitionId  工单模型id
     * @return
     */
    public String eval(String originFormData,String modelDefinitionId) throws Exception {
        return handlerFormData(originFormData,modelDefinitionId);
    }


    /**
     * 组装form-data
     * -- 组装成pipeline组件 需要从外部传入
     * ---- 1。 form-data对应key的schema数据，也就是dosm_model_field_definition 中field_key，field_value_type，field_info字段缓存
     *          model_definiton_id : orderFieldModelInfoMap
     *        Map<String,Map<String, OrderFieldModelInfo> > allFieldInfo = Maps.newConcurrentMap();
     * ---- 2。 sys_user表中的用户id--》name映射缓存数据
     * @param originFormData
     * @return finalFormData
     */
    public  String handlerFormData(String originFormData,String modelDefinitionId) throws Exception{
        Map<String, OrderFieldModelInfo> orderFieldModelInfoMap = getStringOrderFieldModelInfoMap(dosmStatement,modelDefinitionId);

        JSONObject formDataObj = JSONObject.parseObject(originFormData);
        if(null == formDataObj){
            throw new RuntimeException("原始form_data数据为空");
        }
        log.info("-- 原始的form-data数据: {}", formDataObj.toJSONString());
        // 获取JSON中第一层key
        Set<String> keySet = formDataObj.keySet();
        Set<String> distFormDataKeySet = distFormDataKeySetAndRemoveHtmlTags(formDataObj, keySet);
        log.info("-- 剔除HTML标签之后的form-data数据： {}", formDataObj);
        log.info("-- 原始form-data字段值中包含字段个数 : {},字段模型表中包含字段个数 : {}", distFormDataKeySet.size(), orderFieldModelInfoMap.size());

        // 第一步： 统计出来form-data中有但是字段信息表中不存在的字段
        // 差集数据
        List<String> diff = distFormDataKeySet.stream()
                .filter(item -> !orderFieldModelInfoMap
                        .keySet().stream()
                        .collect(Collectors.toList())
                        .contains(item))
                .collect(Collectors.toList());
        log.info("-- 原始form-data存在但是字段信息中不存在的字段 差集数据: {}", diff);

        // 第二步： 处理form-data中需要删除的字段,在Java开发过程中，使用iterator遍历集合的同时对集合进行删除就会出现java.util.ConcurrentModificationException异常
        for (String removeField : REMOVE_FIELDS) {
            formDataObj.remove(removeField);
        }

        // 第三步： 开始组装form-data中有且字段信息表中存在的字段
        // 第四步： 开始处理form-data中有且字段信息表中不存在的字段
        complexHandleFormData(formDataObj, diff, orderFieldModelInfoMap);
        // log.info("-- after complexHandleFormData formDataObj : {}",formDataObj);

        // 第五步： 处理form-data额外添加的字段

        return formDataObj.toJSONString();
    }

    /**
     * form-data数据源组装
     *
     * @param formDataObj
     * @param diff
     * @param orderFieldModelInfoMap
     */
    public  void complexHandleFormData(JSONObject formDataObj, List<String> diff, Map<String, OrderFieldModelInfo> orderFieldModelInfoMap) {
        // form-data JSON的第一层key
        Set<String> keySet = formDataObj.keySet();
        // 处理JSONArray-->String Member类型数据时，额外新增的用户名称信息
        Map<String, JSONArray> memberExtMap = new HashMap<>();

        for (String fieldKey : keySet) {
            // 获取第一层key对应数据的类型
            String typeName = null == formDataObj.get(fieldKey) ? "null" : formDataObj.get(fieldKey).getClass().getTypeName();
            // 单类型key不需要内嵌处理  Long+String
            OrderFieldModelInfo orderFieldModelInfo = orderFieldModelInfoMap.get(fieldKey);

            if (!typeName.equals("com.alibaba.fastjson.JSONArray")) {
                if ("null".equals(typeName)) {
                    log.warn("---form-data中数组字段{} 数据为null", fieldKey);
                    continue;
                }
                // 处理form-data中存在在field字段表中不存在的数据 --不做任何处理
                if (diff.contains(fieldKey)) {
                    log.warn("---字段{} 在dosm_model_field_definition表中对应数据为空", fieldKey);
                    continue;
                }

                // 获取form-data中字段在 字段信息表中的类型来组装数据
                handlerSimpleFieldByFieldValueType(fieldKey, typeName, orderFieldModelInfo, formDataObj, doucStatement);
            } else {
                // 数组类型字段处理：分类两种类型 简单类型数组+JSONObject对象数组<多为表单数据>
                JSONArray jsonArray = formDataObj.getJSONArray(fieldKey);
                if (jsonArray.size() == 0) {
                    log.warn("---form-data中数组字段{} 数据为空", fieldKey);
                    continue;
                }
                String fieldValueType = "";
                JSONArray jsonArrayIds = new JSONArray();
                if (null == orderFieldModelInfo) {
                    log.warn("---字段{} 在dosm_model_field_definition表中对应数据为空", fieldKey);
                    continue;
                }

                fieldValueType = orderFieldModelInfo.getFieldValueType();
                log.debug("--- jsonArray's  fieldValueType : {}",fieldValueType);

                if (fieldValueType.equals(MEMBER.getType())) {
                    jsonArrayIds = new JSONArray(jsonArray.size());
                    memberExtMap.put(fieldKey + "_userId", jsonArrayIds);
                }

                for (int i = 0; i < jsonArray.size(); i++) {
                    // 第一层key对应value是数组，开始遍历，先获取数组元素数据
                    Object fieldKeyOrinVal = jsonArray.get(i);
                    // 原始form-data中fieldKey 对应值
                    String inType = fieldKeyOrinVal.getClass().getTypeName();
                    // java.lang.Long, com.alibaba.fastjson.JSONObject, java.lang.String
                    log.debug("---{} inType: {}", fieldKeyOrinVal, inType);
                    if ("com.alibaba.fastjson.JSONObject".equals(inType)) {
                        // todo JSONObject类型多半是表格数据
                        JSONObject tableObj = (JSONObject) fieldKeyOrinVal;
                        // 获取到FROM_TABLE 字段信息
                        JSONObject tableFieldInfo = orderFieldModelInfo.getFieldInfo();
                        if (Objects.isNull(tableFieldInfo)) {
                            log.warn("---表格字段 {} 在dosm_model_field_definition表中对应字段field_info.colData 数据为空", fieldKey);
                            continue;
                        } else {
                            // 从FROM_TABLE
                            JSONObject tableFieldInfoColData = tableFieldInfo.getJSONObject("colData");
                            if (tableFieldInfoColData.isEmpty()) {
                                log.warn("---表格字段{} 在dosm_model_field_definition表中对应字段field_info.colData 数据为空", fieldKey);
                                continue;
                            }

                            Map<String, FromTableFieldInfo> fromTableFieldInfoMap = Maps.newHashMapWithExpectedSize(tableFieldInfoColData.entrySet().size());
                            // FROM_TABLE 内嵌字段信息组装好（重点是映射性数据）
                            handFromTableInnerFieldInfoWrapperData(fieldKey, tableFieldInfoColData, fromTableFieldInfoMap);

                            // 处理表格内嵌字段数据
                            Map<String, JSONArray> tableInnerKeyExtMap = new HashMap<>();
                            // 遍历表格内嵌JSONOBJECT数据
                            for (String tableInnerKey : tableObj.keySet()) {
                                log.debug("--table->key : {}", tableInnerKey);
                                // 第一步： 从field_info.colDta.k 获取k对应的类型信息等数据
                                if (fromTableFieldInfoMap.containsKey(tableInnerKey)) {
                                    // 第二步： 根据k的类型进行处理
                                    FromTableFieldInfo fromTableFieldInfo = fromTableFieldInfoMap.get(tableInnerKey);
                                    String tableInnerKeyComponentType = fromTableFieldInfo.getComponentType();
                                    if (Objects.isNull(tableInnerKeyComponentType)) {
                                        log.warn("---表格字段{} 在dosm_model_field_definition表中对应字段field_info.colData 类型数据为空 ", fieldKey);
                                        continue;
                                    }

                                    Object tableInnerKeyVal = tableObj.get(tableInnerKey);
                                    if (Objects.nonNull(tableInnerKeyVal)) {
                                        String tableInnerKeyValType = tableInnerKeyVal.getClass().getTypeName();
                                        // 定义在表格字段的field_info.colData.x.enums 映射数据
                                        Map<String, String> tableInnerKeyValEnums = fromTableFieldInfo.getEnums();
                                        log.debug("--kvType : {}", tableInnerKeyValType);
                                        if ("com.alibaba.fastjson.JSONArray".equals(tableInnerKeyValType)) {
                                            log.debug("---------------------tableInnerKey:{}, tableInnerKeyValType: {}", tableInnerKey, tableInnerKeyValType);
                                            JSONArray tableInnerKeyValArrays = (JSONArray) tableInnerKeyVal;
                                            JSONArray jsonArrayIds2 = null;
                                            if (tableInnerKeyComponentType.equals(MEMBER.getType())) {
                                                jsonArrayIds2 = new JSONArray(jsonArray.size());
                                                tableInnerKeyExtMap.put(fieldKey + "_userId", jsonArrayIds2);
                                            }

                                            for (int i1 = 0; i1 < tableInnerKeyValArrays.size(); i1++) {
                                                Object newVal = handlerFromTableInnerFieldByFieldValueType(tableInnerKeyValArrays.get(i1), tableInnerKeyComponentType, doucStatement, tableInnerKeyValEnums);
                                                if (Objects.nonNull(newVal)) {
                                                    if (newVal instanceof List && ((List<?>) newVal).size() >= 2) {
                                                        tableInnerKeyValArrays.set(i1, ((List<?>) newVal).get(1));
                                                        jsonArrayIds2.set(i1, ((List<?>) newVal).get(0));
                                                    } else {
                                                        tableInnerKeyValArrays.set(i1, newVal);
                                                    }
                                                }
                                            }
                                        } else {
                                            Object newVal = handlerFromTableInnerFieldByFieldValueType(tableInnerKeyVal, tableInnerKeyComponentType, doucStatement, tableInnerKeyValEnums);
                                            if (Objects.nonNull(newVal)) {
                                                tableObj.put(tableInnerKey, newVal);
                                            }
                                        }
                                    }
                                } else {
                                    log.warn("---表格字段{} 在dosm_model_field_definition表中对应字段field_info.colData 数据为空 2 ", fieldKey);
                                    continue;
                                }
                            }
                            if (tableInnerKeyExtMap.size() > 0) {
                                tableObj.putAll(tableInnerKeyExtMap);
                            }
                        }
                    } else {
                        // 无需特殊处理的字段，直接返回
                        if (isNoHandleType(orderFieldModelInfo.getFieldValueType())) {
                            continue;
                        }
                        String newVal = handlerArraysSimpleValueType(fieldKey, inType, fieldKeyOrinVal, orderFieldModelInfo, doucStatement);
                        if (Objects.nonNull(newVal)) {
                            jsonArray.set(i, newVal);
                            if (fieldValueType.equals(MEMBER.getType())) {
                                jsonArrayIds.set(i, fieldKeyOrinVal);
                            }
                        }
                    }
                }
                formDataObj.put(fieldKey, jsonArray);
            }
        }
        if (memberExtMap.size() > 0) {
            formDataObj.putAll(memberExtMap);
        }
    }
}
