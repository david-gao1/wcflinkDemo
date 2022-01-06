package com.dodp.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author richard.duo
 * @version 1.0.0
 * @ClassName TransformFunction.java
 * @Description 模拟clickhouse中的transform函数
 * @createTime 2021年12月26日 11:49:00
 */
public class ClickHouseTransformFunction extends ScalarFunction {

    /**
     * 数据转换
     * transform(workOrder.order_status,['FINISHED','PROCESSING','BE_PROCESSED','CLOSED'],[ '已完成','处理中','待领取','已关闭'],'其他')
     * @param originKey         原本key
     * @param keys              key候选项
     * @param values            value映射值候选项
     * @param defaultValue      未匹配的默认值
     * @return
     */
    public String eval(String originKey,String[] keys,String[] values,String defaultValue){
        if(null == originKey){
            throw new IllegalArgumentException("originKey 必须不为空");
        }
        if(null == keys || keys.length == 0){
            throw new IllegalArgumentException("keys 必须不为空");
        }
        if(null == defaultValue){
            throw new IllegalArgumentException("defaultValue 必须不为空");
        }

        Map<String,String> mapping = new HashMap<>();
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            String value = defaultValue;
            int j = i;
            if(null != values && values.length >= (j+1)){
                value = values[i];
            }
            mapping.put(key,value);
        }

        if(mapping.containsKey(originKey)){
            return mapping.get(originKey);
        }
        return defaultValue;
    }
}
