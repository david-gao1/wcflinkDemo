package com.gao.flink.sql.dataTypes;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * @Description flink的类
 * @Author lianggao
 * @Date 2021/5/17 10:45 上午
 * @Version 1.0
 */
public class FlinkDataTypes {
    public static void main(String[] args) {
        //todo:使用DataTypes进行类型的嵌套类型
        DataType array1 = DataTypes.ARRAY(DataTypes.ROW());
        System.out.println(array1.getLogicalType());

    }
}
