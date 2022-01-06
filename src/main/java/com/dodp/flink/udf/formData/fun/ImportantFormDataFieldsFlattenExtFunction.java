package com.dodp.flink.udf.formData.fun;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.dodp.flink.udf.formData.utils.DataHandlerUtils.getValueFromDataJSON;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName FormDataFieldLatternExtFunction.java
 * @Description form_data 重组之后字段拉平+冗余出来
 * -- 仅适用于重大事件 处理
 * @createTime 2021年12月24日 11:32:00
 */
@Slf4j
public class ImportantFormDataFieldsFlattenExtFunction extends TableFunction<Row> {

    /**
     * 根据form_data中数据进行拉平+扩展
     * @param formData
     */
    @DataTypeHint("ROW<problemstate  STRING,breakstarttime  STRING,liabilitydepartment  STRING,liabilitygroup  STRING,reasonclass  STRING," +
            "reasonsubclass  STRING,CHorRL  STRING, systemname  STRING,systemgrade  STRING,belongdepartment  STRING,systemnumber  STRING,affectedinformation  STRING," +
            "improvemeasurelevel  STRING,improvemeasuretype  STRING,turnproblem  STRING,responsegroup  STRING,responseperson  STRING,plancompletetime  STRING," +
            "responsedepartment  STRING,infrastructureownership  STRING,onelevelservicedirectory  STRING,twolevelservicedirectory  STRING,unavailabletime STRING>")
    public void eval(String formData){
        JSONObject finalFormData = JSONObject.parseObject(formData);

        int extraSize = 0;
        String nullVal = "";
        List<Row> rows = new ArrayList<>();

        Set<Map.Entry<String, String>> entries = TABLE_DATA_CHILD_MAP.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            String key = entry.getKey();
            Object o = finalFormData.get(key);
            if (o instanceof JSONArray) {
                JSONArray jsonArray = finalFormData.getJSONArray(key);
                if (jsonArray != null && jsonArray.size() > 0) {
                    for (Object obj : jsonArray) {
                        JSONObject jsonObject = (JSONObject) obj;
                        switch (key) {
                            case FROM_TABLE_AFFECTE_DINFORMATION:
                                rows.add(Row.of(getValueFromDataJSON("problemstate", finalFormData), getValueFromDataJSON("breakstarttime", finalFormData), getValueFromDataJSON("liabilitydepartment", finalFormData), getValueFromDataJSON("liabilitygroup", finalFormData), getValueFromDataJSON("reasonclass", finalFormData), getValueFromDataJSON("reasonsubclass", finalFormData), getValueFromDataJSON("CHorRL", finalFormData),
                                        getValueFromDataJSON("systemname", jsonObject),getValueFromDataJSON("systemgrade", jsonObject),getValueFromDataJSON("belongdepartment", jsonObject), getValueFromDataJSON("systemnumber", jsonObject),getValueFromDataJSON("serviceaffectedtime", jsonObject),
                                        nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal,
                                        nullVal, nullVal, nullVal, nullVal));
                                        break;
                            case FROM_TABLE_IMPROVE_MEASURE:
                               rows.add(Row.of(getValueFromDataJSON("problemstate", finalFormData), getValueFromDataJSON("breakstarttime", finalFormData), getValueFromDataJSON("liabilitydepartment", finalFormData), getValueFromDataJSON("liabilitygroup", finalFormData), getValueFromDataJSON("reasonclass", finalFormData), getValueFromDataJSON("reasonsubclass", finalFormData), getValueFromDataJSON("CHorRL", finalFormData),
                                       nullVal,nullVal,nullVal, nullVal,nullVal,
                                       getValueFromDataJSON("improvemeasurelevel", jsonObject), getValueFromDataJSON("improvemeasuretype", jsonObject), getValueFromDataJSON("turnproblem", jsonObject), getValueFromDataJSON("responsegroup", jsonObject), getValueFromDataJSON("responseperson", jsonObject), getValueFromDataJSON("plancompletetime", jsonObject), getValueFromDataJSON("responsedepartment", jsonObject),
                                       nullVal, nullVal, nullVal, nullVal
                                       ));

                                break;
                            case FROM_TABLE_FAULT_BOTTOM_COMPONENT:
                                rows.add(Row.of(getValueFromDataJSON("problemstate", finalFormData), getValueFromDataJSON("breakstarttime", finalFormData), getValueFromDataJSON("liabilitydepartment", finalFormData), getValueFromDataJSON("liabilitygroup", finalFormData), getValueFromDataJSON("reasonclass", finalFormData), getValueFromDataJSON("reasonsubclass", finalFormData), getValueFromDataJSON("CHorRL", finalFormData),
                                        nullVal,nullVal,nullVal, nullVal,nullVal,
                                        nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal,
                                        getValueFromDataJSON("infrastructureownership", jsonObject), getValueFromDataJSON("onelevelservicedirectory", jsonObject), getValueFromDataJSON("twolevelservicedirectory", jsonObject), getValueFromDataJSON("unavailabletime", jsonObject)
                                ));
                                break;
                        }
                        extraSize++;
                    }
                }
            }else{
                String typeName = null;
                try {
                    typeName = o.getClass().getTypeName();
                } catch (Exception e) {
                    log.error("------error: {}",e.getLocalizedMessage());
                }

                log.debug(typeName);
            }
        }
        // 应对当前数据中没有对应的需要拆分的字表数据时，插入最基础的数据
        if (extraSize == 0) {
            rows.add(Row.of(getValueFromDataJSON("problemstate", finalFormData), getValueFromDataJSON("breakstarttime", finalFormData), getValueFromDataJSON("liabilitydepartment", finalFormData), getValueFromDataJSON("liabilitygroup", finalFormData), getValueFromDataJSON("reasonclass", finalFormData), getValueFromDataJSON("reasonsubclass", finalFormData), getValueFromDataJSON("CHorRL", finalFormData),
                    nullVal,nullVal,nullVal, nullVal,nullVal,
                    nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal,
                    nullVal, nullVal, nullVal, nullVal
            ));
        }

        rows.forEach(row -> collect(row));

    }


    /**
     * 扩展字段数据来源
     * -- key： 扩展字段名称  value： 字段在form-data中位置
     */
    public static final Map<String,String> EXTRA_FIELD_SOURCE_MAPPING = Maps.newHashMap();
    public static final Map<String,String> TABLE_DATA_CHILD_MAP = Maps.newHashMap();
    public static final String FROM_TABLE_AFFECTE_DINFORMATION = "affectedinformation";
    public static final String FROM_TABLE_IMPROVE_MEASURE= "improvemeasure";
    public static final String FROM_TABLE_FAULT_BOTTOM_COMPONENT= "faultbottomcomponent";
    public static final String[] FROM_TABLE_EXPLODED = new String[]{FROM_TABLE_AFFECTE_DINFORMATION,FROM_TABLE_IMPROVE_MEASURE,FROM_TABLE_FAULT_BOTTOM_COMPONENT};

    static {
        EXTRA_FIELD_SOURCE_MAPPING.put("problemstate","problemstate");
        EXTRA_FIELD_SOURCE_MAPPING.put("breakstarttime","breakstarttime");
        EXTRA_FIELD_SOURCE_MAPPING.put("liabilitydepartment","liabilitydepartment");
        EXTRA_FIELD_SOURCE_MAPPING.put("liabilitygroup","liabilitygroup");
        EXTRA_FIELD_SOURCE_MAPPING.put("reasonclass","reasonclass");
        EXTRA_FIELD_SOURCE_MAPPING.put("reasonsubclass","reasonsubclass");
        EXTRA_FIELD_SOURCE_MAPPING.put("CHorRL","CHorRL");
        EXTRA_FIELD_SOURCE_MAPPING.put("systemname",FROM_TABLE_EXPLODED[0]+".systemname");
        EXTRA_FIELD_SOURCE_MAPPING.put("systemgrade",FROM_TABLE_EXPLODED[0]+".systemgrade");
        EXTRA_FIELD_SOURCE_MAPPING.put("belongdepartment",FROM_TABLE_EXPLODED[0]+".belongdepartment");
        EXTRA_FIELD_SOURCE_MAPPING.put("systemnumber",FROM_TABLE_EXPLODED[0]+".systemnumber");
        EXTRA_FIELD_SOURCE_MAPPING.put("affectedinformation",FROM_TABLE_EXPLODED[0]+".serviceaffectedtime");
        EXTRA_FIELD_SOURCE_MAPPING.put("improvemeasurelevel",FROM_TABLE_EXPLODED[1]+".improvemeasurelevel");
        EXTRA_FIELD_SOURCE_MAPPING.put("improvemeasuretype",FROM_TABLE_EXPLODED[1]+".improvemeasuretype");
        EXTRA_FIELD_SOURCE_MAPPING.put("turnproblem",FROM_TABLE_EXPLODED[1]+".turnproblem");
        EXTRA_FIELD_SOURCE_MAPPING.put("responsegroup",FROM_TABLE_EXPLODED[1]+".responsegroup");
        EXTRA_FIELD_SOURCE_MAPPING.put("responseperson",FROM_TABLE_EXPLODED[1]+".responseperson");
        EXTRA_FIELD_SOURCE_MAPPING.put("plancompletetime",FROM_TABLE_EXPLODED[1]+".plancompletetime");
        EXTRA_FIELD_SOURCE_MAPPING.put("responsedepartment",FROM_TABLE_EXPLODED[1]+".responsedepartment");
        EXTRA_FIELD_SOURCE_MAPPING.put("infrastructureownership",FROM_TABLE_EXPLODED[2]+".infrastructureownership");
        EXTRA_FIELD_SOURCE_MAPPING.put("onelevelservicedirectory",FROM_TABLE_EXPLODED[2]+".onelevelservicedirectory");
        EXTRA_FIELD_SOURCE_MAPPING.put("twolevelservicedirectory",FROM_TABLE_EXPLODED[2]+".twolevelservicedirectory");
        EXTRA_FIELD_SOURCE_MAPPING.put("unavailabletime",FROM_TABLE_EXPLODED[2]+".unavailabletime");

        TABLE_DATA_CHILD_MAP.put(FROM_TABLE_EXPLODED[0],"systemname,systemgrade,belongdepartment,systemnumber,serviceaffectedtime");
        TABLE_DATA_CHILD_MAP.put(FROM_TABLE_EXPLODED[1],"improvemeasurelevel,improvemeasuretype,turnproblem,responsegroup,responseperson,plancompletetime,responsedepartment");
        TABLE_DATA_CHILD_MAP.put(FROM_TABLE_EXPLODED[2],"infrastructureownership,onelevelservicedirectory,twolevelservicedirectory,unavailabletime");
    }

}
