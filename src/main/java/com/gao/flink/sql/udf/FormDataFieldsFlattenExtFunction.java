package com.gao.flink.sql.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.util.Optional;

/**
 * @Description 重组之后字段拉平+冗余出来
 * @Author lianggao
 * @Date 2022/1/11 上午11:19
 */
//@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class FormDataFieldsFlattenExtFunction extends TableFunction<Row> {


    public static final String INTEGER = "INTEGER";
    public static final String INT = "INT";
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String STRING = "STRING";


    /**
     * 说明用例
     * <p>
     * 1、输入值可以是一个或多个
     * 2、输出：
     * 输出可以输出任意多行
     * 返回类型取决于 TableFunction 类的泛型参数 T
     * 不包含返回类型，通过 collect(T) 方法来发送要输出的行。如下：
     *
     * @param str：被拉平的数据
     * @param type:拉平的类型
     */
    public void eval(String str, String type) {
        //一条数据比如"111 222 333"，切分之后会发送三行
        for (String s : str.split(" ")) {
            // use collect(...) to emit a row
            collect(Row.of(s, s.length()));
        }
    }

    /**
     * 设置输入输出类型
     *
     * @param typeFactory
     * @return
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                // 指定输入参数的类型，必要时参数会被隐式转换
                //.typedArguments(DataTypes.STRING(), DataTypes.STRING())
                // specify a strategy for the result data type of the function
                .outputTypeStrategy(
                        // 基于字符串值返回数据类型
                        FormDataFieldsFlattenExtFunction::inferType
                )
                .build();
    }


    /**
     * 动态地，根据sql传来的字段类型信息，设置输出的行信息
     *
     * @param callContext
     * @return
     */
    private static Optional<DataType> inferType(CallContext callContext) {
        //1、从sql中获取拉平冗余类型string
        //"1"：从函数第二个位置获取字段类型设置
        //可获取设置字串，如'newWord string,newLength int'
        final String fieldsSchema = callContext.getArgumentValue(1, String.class).orElse(STRING);

        //2、设置字段名和类型
        String[] fieldAndTypes = fieldsSchema.split(",");
        String[] fieldNames = new String[fieldAndTypes.length];
        DataType[] dataTypes = new DataType[fieldAndTypes.length];
        int pos = 0;
        for (String fieldAndTypeS : fieldAndTypes) {
            String[] fieldAndType = fieldAndTypeS.split(" ");
            fieldNames[pos] = fieldAndType[0];
            String typeName = fieldAndType[1].toUpperCase();
            switch (typeName) {
                case INT:
                    dataTypes[pos] = DataTypes.INT();
                    break;
                case STRING:
                    dataTypes[pos] = DataTypes.STRING();
                    break;
                case DATE:
                    dataTypes[pos] = DataTypes.DATE();
                    break;
                case DATETIME:
                    dataTypes[pos] = DataTypes.TIMESTAMP();
                    break;
                default:
                    dataTypes[pos] = DataTypes.STRING();
            }
            pos++;
        }

        //3、设置拉平冗余字段
        DataType dataType = TableSchema.builder().fields(fieldNames, dataTypes).build().toRowDataType();
        return Optional.of(dataType);
    }
}

