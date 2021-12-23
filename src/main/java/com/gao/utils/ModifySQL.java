package com.gao.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description 识别sql中的中文并添加字符编码
 * @Author lianggao
 * @Date 2021/11/23 下午4:16
 * @Version 1.0
 */
public class ModifySQL {

    public final static String QUOTATION_MARKS_REGEX = "(.*'.*'.*)";


    public static void main(String[] args) {
        String sql = "INSERT INTO `test_gao_sink`\n" +
                " (SELECT *\n" +
                " FROM `test_gao_source`\n" +
                " WHERE `type` <> 'ni哈哈11' and type = 'n是的ihao');";
        System.out.println(getSQLWithModify(sql));

    }


    /**
     * 预处理sql
     * 如果host中存在; 先处理掉,防止;切分sql的时候出现错误
     *
     * @param sql
     */
    public static String getSQLWithModify(String sql) {
        Pattern hostCompile = Pattern.compile(QUOTATION_MARKS_REGEX);
        Matcher matcher = hostCompile.matcher(sql);
        String sqlTurn = sql;
        while (matcher.find()) {
            System.out.println("catch chinese");
            String group = matcher.group(1);
            if (isContainChinese(group)){
                sqlTurn = sqlTurn.replace(group, "_UTF16" + group);
            }

        }
        return sqlTurn;
    }


    public static boolean isContainChinese(String str) {
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(str);
        return m.find();
    }

}
