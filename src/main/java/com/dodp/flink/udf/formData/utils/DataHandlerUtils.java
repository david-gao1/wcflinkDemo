package com.dodp.flink.udf.formData.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dodp.flink.udf.formData.entity.FromTableFieldInfo;
import com.dodp.flink.udf.formData.entity.OrderFieldModelInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.dodp.flink.udf.conts.Constants.*;
import static com.dodp.flink.udf.formData.entity.FieldValueType.*;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName DataHandlerUtils.java
 * @Description 重组form_data数据的工具类
 * @createTime 2021年12月18日 18:07:00
 */
@Slf4j
public class DataHandlerUtils {

    /**
     * 根据用户id获取用户名称
     *
     * @return
     */
    public static String getSysUserName(Long userId, Statement doucStatement) {
        String userName = "";
        String sql = String.format(SYS_USER_NAME_SQL, DOUC_DB_NAME,userId);
        try (ResultSet resultSet = doucStatement.executeQuery(sql)) {
            while (resultSet.next()) {
                userName = resultSet.getString("name");
                if (Objects.nonNull(userName)) {
                    break;
                }
            }
        } catch (SQLException e) {
            log.error("--getSysUserName occur error : {} {}", e.getErrorCode(), e.getLocalizedMessage());
        } finally {
            return userName;
        }
    }


    /**
     * 表格外数据｜form-data第一层key--处理简单类型的field字段
     *
     * @param fieldKey
     * @param typeName
     * @param orderFieldModelInfo
     * @param formDataObj
     */
    public static void handlerSimpleFieldByFieldValueType(String fieldKey, String typeName, OrderFieldModelInfo orderFieldModelInfo,
                                                          JSONObject formDataObj, Statement doucStatement) {
        // dosm定义的17种业务数据类型
        String fieldValueType = orderFieldModelInfo.getFieldValueType();
        log.debug("--form-data 中字段： {} <类型 : {}>，在字段信息表中定义的值类型为 ： {}",fieldKey,typeName,fieldValueType);

        // 无需特殊处理的字段，直接返回
        if (isNoHandleType(fieldValueType)) {
            if (typeName.equals("java.lang.String")) {
                //  \"zzz\"
                formDataObj.put(fieldKey,removeHtmlTags(formDataObj.getString(fieldKey)).replaceAll("\"",""));
            }
            return;
        }

        // MEMBER类型的需要扩展用户名称信息
        if (fieldValueType.equals(MEMBER.getType())) {
            try {
                Long userId = 0L;
                if (typeName.equals("java.lang.String")) {
                    userId = Long.parseLong(formDataObj.getString(fieldKey));
                } else {
                    userId = formDataObj.getLong(fieldKey);
                }
                String sysUserName = getSysUserName(userId, doucStatement);
                if (Objects.nonNull(sysUserName)) {
                    formDataObj.put(fieldKey + "_userId", userId);
                    formDataObj.put(fieldKey, sysUserName);
                }
            } catch (NumberFormatException e) {
                log.error("---handlerSimpleFieldByFieldValueType handler {} type field  value : {} error : {}",fieldValueType,formDataObj.get(fieldKey),e.getLocalizedMessage());

            }
        }

        // DATE 类型将Long时间戳转化为String指定的SDF格式
        if (fieldValueType.equals(DATE.getType())) {
            try {
                Long time = 0L;
                if (typeName.equals("java.lang.String")) {
                    time = Long.parseLong(formDataObj.getString(fieldKey));
                } else {
                    time = formDataObj.getLong(fieldKey);
                }
                if (time.toString().length() == 10) {
                    time = time * 1000;
                }
                String format = SDF.format(new Date(time));
                formDataObj.put(fieldKey, format);
            } catch (Exception e) {
                log.error("---handlerSimpleFieldByFieldValueType handler {} type field  value : {} error : {}",fieldValueType,formDataObj.get(fieldKey),e.getLocalizedMessage());
            }
        }

        // 映射型字段处理
        if (isMappingHandleType(fieldValueType)) {
            try {
                Map<String, String> fieldKeyLabels = handleFileInfoGetKeyMappingLabels(orderFieldModelInfo.getFieldInfo().toJSONString());
                String originKeyValue = "";
                if (typeName.equals("java.lang.String")) {
                    originKeyValue = formDataObj.getString(fieldKey);
                } else {
                    originKeyValue = String.valueOf(formDataObj.getLong(fieldKey));
                }
                formDataObj.put(fieldKey, fieldKeyLabels.get(originKeyValue));
            } catch (Exception e) {
                log.error("---handlerSimpleFieldByFieldValueType handler {} type field  value : {} error : {}",fieldValueType,formDataObj.get(fieldKey),e.getLocalizedMessage());
            }
        }
    }

    /**
     * 数组内部--简单类型数据处理
     *
     * @param fieldKey
     * @param typeName
     * @param orderFieldModelInfo
     * @param doucStatement
     */
    public static String handlerArraysSimpleValueType(String fieldKey, String typeName, Object originValue,
                                                      OrderFieldModelInfo orderFieldModelInfo, Statement doucStatement) {
        // 业务数据类型--17种之一
        String fieldValueType = orderFieldModelInfo.getFieldValueType();

        // MEMBER类型的需要扩展用户名称信息
        if (fieldValueType.equals(MEMBER.getType())) {
            try {
                Long userId = 0L;
                if (typeName.equals("java.lang.String")) {
                    userId = Long.parseLong(originValue.toString());
                } else {
                    userId = (Long) originValue;
                }
                String sysUserName = getSysUserName(userId, doucStatement);
                return sysUserName;
            } catch (NumberFormatException e) {
                log.error("---handlerArraysSimpleValueType handler {} type field  value : {} error : {}",fieldValueType,originValue,e.getLocalizedMessage());
            }
        }

        // DATE 类型将时间戳转化为SDF格式
        if (fieldValueType.equals(DATE.getType())) {
            try {
                Long time = 0L;
                if (typeName.equals("java.lang.String")) {
                    try {
                        time = Long.parseLong(originValue.toString());
                    } catch (Exception e) {
                        log.error("-- Long.parseLong({}) occur error {}", originValue, e.getLocalizedMessage());
                    }
                } else {
                    time = (Long) originValue;
                }
                if (time.toString().length() == 10) {
                    time = time * 1000;
                }
                String format = SDF.format(new Date(time));
                return format;
            } catch (Exception e) {
                log.error("---handlerArraysSimpleValueType handler {} type field  value : {} error : {}",fieldValueType,originValue,e.getLocalizedMessage());
            }
        }

        // 映射型字段处理
        if (isMappingHandleType(fieldValueType)) {
            try {
                Map<String, String> fieldKeyLabels = handleFileInfoGetKeyMappingLabels(orderFieldModelInfo.getFieldInfo().toJSONString());
                String originKeyValue = "";
                if (typeName.equals("java.lang.String")) {
                    originKeyValue = (String) originValue;
                } else {
                    originKeyValue = originKeyValue.toString();
                }
                return fieldKeyLabels.get(originKeyValue);
            } catch (Exception e) {
                log.error("---handlerArraysSimpleValueType handler {} type field  value : {} error : {}",fieldValueType,originValue,e.getLocalizedMessage());
            }
        }
        return null;
    }

    /**
     * EVALUATE_STAR 评分数据处理
     *
     * @param fieldKey
     * @param orderFieldModelInfo
     * @param formDataObj
     */
    public static void handlerEvaluateStarTypeField(String fieldKey, OrderFieldModelInfo orderFieldModelInfo, JSONObject formDataObj) {

    }

    /**
     * 从表dosm_model_field_defition表中字段field_info 获取(field_key'value -> label)映射关系
     *
     * @param fieldInfo
     * @return Map<field_key ' value, label> 字段field_key的值v作为key，字段值v对应的映射值m 作value的Map集合
     * @throws Exception
     */
    public static Map<String, String> handleFileInfoGetKeyMappingLabels(String fieldInfo) throws Exception {
        Map<String, String> keyValues = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(fieldInfo);

        // 处理enum数据
        JsonNode enums = jsonNode.get("enum");
        deepHandlerChild(enums, keyValues);
        return keyValues;
    }

    /**
     * 递归遍历获取所有层次数据
     *
     * @param node
     * @param keyValues
     */
    private static void deepHandlerChild(JsonNode node, Map<String, String> keyValues) throws Exception {
        Iterator<JsonNode> iterator = node.iterator();
        while (iterator.hasNext()) {
            node = iterator.next();
            String key = node.get("value").asText();
            String label = node.get("label").asText();
            // log.info("key -> label : "+ key+" -> "+ label);
            keyValues.put(key, label);
            JsonNode childrenNode = node.get("children");
            if (!childrenNode.isNull()) {
                deepHandlerChild(childrenNode, keyValues);
            }
        }
    }

    /**
     * 处理FROM_TABLE类型字段中内嵌数据
     *
     * @param fieldKey              表格key
     * @param colData               表格key对应在dosm_model_field_definition表中的field_info.colData字段值
     * @param fromTableFieldInfoMap 需要采集数据的Map
     */
    public static void handFromTableInnerFieldInfoWrapperData(String fieldKey, JSONObject colData, Map<String, FromTableFieldInfo> fromTableFieldInfoMap) {
        for (String colDataKey : colData.keySet()) {
            log.debug("----正在处理表格{} 内嵌的字段{} 的数据 ", fieldKey, colDataKey);
            JSONObject colDataVal = colData.getJSONObject(colDataKey);
            if (colDataVal.isEmpty()) {
                log.warn("----处理表格{} 内嵌的字段{} 的数据 为空 ", fieldKey, colDataKey);
                continue;
            }
            JSONObject colDataKeyVal = colData.getJSONObject(colDataKey);
            String componentType = colDataKeyVal.getString("componentType");
            String title = colDataKeyVal.getString("title");

            FromTableFieldInfo info = new FromTableFieldInfo();
            info.setFieldKey(colDataKey);
            info.setComponentType(componentType);
            info.setTitle(title);
            fromTableFieldInfoMap.put(colDataKey,info);

            // 组装映射类型的数据备用值
            if (isMappingHandleType(componentType)) {
                try {
                    Map<String, String> enums = handleFileInfoGetKeyMappingLabels(colDataKeyVal.toJSONString());
                    info.setEnums(enums);
                } catch (Exception e) {
                    log.error("-- ----处理表格{} 内嵌的SELECT类型字段{} 的数据出错 {}",e.getLocalizedMessage());
                }
            }
        }
    }

    /**
     * 获取原始form-data中所有字段信息
     * -- 进行字段去重
     * -- String类型值去除HTML标签
     *
     * @param formDataObj
     * @param keySet
     * @return
     */
    public static Set<String> distFormDataKeySetAndRemoveHtmlTags(JSONObject formDataObj, Set<String> keySet) {
        Set<String> distFormDataKey = new HashSet<>();
        // 统计所有的类型
        Set<String> fieldTypes = new HashSet<>();
        // 第〇步： 遍历统计出来form-data字段中所有的key
        for (String key : keySet) {
            // 获取key对应value值的类型 需要针对  "changedKey":null, 做特殊处理
            String typeName = null == formDataObj.get(key) ? "null" : formDataObj.get(key).getClass().getTypeName();
            fieldTypes.add(typeName);
            // 单类型key不需要内嵌处理  Long+String
            if (!typeName.equals("com.alibaba.fastjson.JSONArray")) {
                distFormDataKey.add(key);
                // 同时将字符串中的HTML标签剔除
                if (typeName.equals("java.lang.String")) {
                    String val = formDataObj.getString(key);
                    formDataObj.put(key, removeHtmlTags(val));
                }
            } else {
                // log.info("key: {}, valueType: {}",key ,typeName);
                /*  数组类型的需要统计内部可能存在的字段
                    -- prioritylevel： 简单类型字段
                    -- prioritylevel： 简单类型数组，里面数据是单类型
                    -- treatmentform:  复杂类型数组，特殊组件-表格，里面是多个个结构体一致的数据，结构体内不会再嵌套json

                   "prioritylevel":"439333228",
                   "emergencyplan":["686392260"],
                    "treatmentform":[
                        {
                            "key":"d34dd54b5411e5b8761d6cf6066443c2",
                            "endingtime":1637738100000,
                            "handlestage":[
                                "550991392"
                            ],
                            "personliable":[
                                "427960"
                            ],
                            "startingtime":1637738100000,
                            "handleprocesstext":"监控告警虚拟机异常"
                        },
                    ] */
                distFormDataKey.add(key);
                JSONArray jsonArray = formDataObj.getJSONArray(key);
                for (Object o : jsonArray) {
                    String inType = o.getClass().getTypeName();
                    // java.lang.Long, com.alibaba.fastjson.JSONObject, java.lang.String
                    fieldTypes.add(inType);
                    if ("com.alibaba.fastjson.JSONObject".equals(inType)) {
                        JSONObject obj = (JSONObject) o;
                        for (String s : obj.keySet()) {
                            distFormDataKey.add(s);
                            // 同时将字符串中的HTML标签剔除
                            if (typeName.equals("java.lang.String")) {
                                String val = formDataObj.getString(key);
                                formDataObj.put(key, removeHtmlTags(val));
                            }
                        }
                    }
                }
            }
        }
        return distFormDataKey;
    }

    /**
     * 去除html代码中含有的标签
     *
     * @param htmlStr
     * @return
     */
    public static String removeHtmlTags(String htmlStr) {
        // 定义script的正则表达式，去除js可以防止注入
        String scriptRegex = "<script[^>]*?>[\\s\\S]*?<\\/script>";
        // 定义style的正则表达式，去除style样式，防止css代码过多时只截取到css样式代码
        String styleRegex = "<style[^>]*?>[\\s\\S]*?<\\/style>";
        // 定义HTML标签的正则表达式，去除标签，只提取文字内容
        String htmlRegex = "<[^>]+>";
        // 定义空格,回车,换行符,制表符
        String spaceRegex = "\t|\r|\n";
        String spaceRege2x = "\\t|\\r|\\n";
        htmlStr = htmlStr.replaceAll("&lt;", "<");
        htmlStr = htmlStr.replaceAll("&gt;", ">");
        // 过滤script标签
        htmlStr = htmlStr.replaceAll(scriptRegex, " ");
        // 过滤style标签
        htmlStr = htmlStr.replaceAll(styleRegex, " ");
        // 过滤html标签
        htmlStr = htmlStr.replaceAll(htmlRegex, " ");
        // 过滤空格等
         htmlStr = htmlStr.replaceAll("\t", "");
         htmlStr = htmlStr.replaceAll("\n", "");
         htmlStr = htmlStr.replaceAll("\r", "");

        htmlStr = htmlStr.replaceAll("&nbsp;", " ");
        htmlStr = htmlStr.replaceAll("`", "");
        htmlStr = htmlStr.replaceAll("\'", "\"");
        return htmlStr;
    }

    /**
     * 处理FROM_TABLE 内嵌字段
     * @param val
     * @param tableInnerKeyValType
     * @param doucStatement
     * @param tableInnerKeyValEnums
     */
    public static Object handlerFromTableInnerFieldByFieldValueType(Object val, String tableInnerKeyValType, Statement doucStatement,Map<String, String> tableInnerKeyValEnums) {

        // 无需特殊处理的字段，直接返回
        if (isNoHandleType(tableInnerKeyValType)) {
            if(val instanceof String){
                return removeHtmlTags((String)val).replaceAll("\"","");
            }
            return null;
        }

        // MEMBER类型的需要扩展用户名称信息
        if (tableInnerKeyValType.equals(MEMBER.getType())) {
            try {
                Long userId = Long.parseLong(val.toString());
                String sysUserName = getSysUserName(userId, doucStatement);
                List<Object> objects = Lists.newArrayList();

                if (Objects.nonNull(sysUserName)) {
                    objects.add(userId);
                    objects.add(sysUserName);
                }
                return objects;
            } catch (NumberFormatException e) {
                log.error("---handlerFromTableInnerFieldByFieldValueType handler {} type field  value : {} error : {}",tableInnerKeyValType,val,e.getLocalizedMessage());
            }
        }

        // DATE 类型将Long时间戳转化为String指定的SDF格式
        if (tableInnerKeyValType.equals(DATE.getType())) {
            Long time = 0L;
            try {
                time = Long.parseLong(val.toString());
                if (time.toString().length() == 10) {
                    time = time * 1000;
                }
                String format = SDF.format(new Date(time));
                return format;
            } catch (Exception e) {
                log.error("---handlerFromTableInnerFieldByFieldValueType handler {} type field  value : {} error : {}",tableInnerKeyValType,val,e.getLocalizedMessage());
            }
        }

        // 映射型字段处理
        if (isMappingHandleType(tableInnerKeyValType)) {
            try {
                String originKeyValue = val.toString();
                return tableInnerKeyValEnums.get(originKeyValue);
            } catch (Exception e) {
                log.error("---handlerFromTableInnerFieldByFieldValueType handler {} type field  value : {} error : {}",tableInnerKeyValType,val,e.getLocalizedMessage());
            }
        }
        return null;
    }

    /**
     * 将JSON中只有单个String值的数组转换为String
     * @param key
     * @param jsonObject
     * @return
     */
    public static String getValueFromDataJSON(String key,JSONObject jsonObject){
        if (jsonObject.containsKey(key)) {
            Object o = jsonObject.get(key);
            if(o instanceof JSONArray ){
                JSONArray arr = (JSONArray) o;
                if(arr.size() == 0){
                    return "";
                }
                if(arr.size() == 1){
                    return arr.getString(0);
                }
                return arr.toJSONString();
            }
            return jsonObject.getString(key);
        }else{
            return "";
        }
    }

    public static String getValueFromDataJSON(JSONObject jsonObject,String key){
        return getValueFromDataJSON(key,jsonObject);
    }


    /**
     *  组装form-data中key在dosm_model_field_definition 中
     * @param dosmStatement
     * @param modelDefinitionId
     * @return
     * @throws SQLException
     */
    public static Map<String, OrderFieldModelInfo> getStringOrderFieldModelInfoMap(Statement dosmStatement, String modelDefinitionId) throws SQLException {
        String fieldInfoFinSQL = String.format(FIELD_INFO_SQL, modelDefinitionId);
        log.debug("--model_definition_id : " + modelDefinitionId);
        ResultSet rs = dosmStatement.executeQuery(fieldInfoFinSQL);
        Map<String, OrderFieldModelInfo> orderFieldModelInfoMap = new HashMap<>(100);
        while (rs.next()) {
            OrderFieldModelInfo info = new OrderFieldModelInfo();
            info.setModelDefinitionId(rs.getString("model_definition_id"));
            info.setFieldCode(rs.getString("field_code"));
            String field_key = rs.getString("field_key");
            info.setFieldKey(field_key);
            info.setFieldName(rs.getString("field_name"));
            info.setFieldValueType(rs.getString("field_value_type"));
            info.setFieldInfo(JSON.parseObject(rs.getString("field_info")));
            orderFieldModelInfoMap.put(field_key, info);
        }
        rs.close();
        rs = null;
        return orderFieldModelInfoMap;
    }
}
