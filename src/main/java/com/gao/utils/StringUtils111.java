package com.gao.utils;

import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/5/19 9:04 下午
 * @Version 1.0
 */
public class StringUtils111 {
    public static void main(String[] args) {
        //regex1();

        //System.out.println("`job[]`");
        //System.out.println(remove());

        //String s = addSpecialAvoidKeyWords("nihao[1]");
        //System.out.println(s);

        //String s1 = "`1212`";

        //System.out.println(s1.startsWith("`")&&s1.endsWith("`"));
        int random = (int)(Math.random()*100);
        System.out.println(random);

    }

    private static void regex1() {
        String regex = ".*\\[\\w+\\]\\..*";
        String s = "12121nihao[1121].12121212";
        System.out.println(s.matches(regex));
    }

    private static String remove() {
        String modifiedField = "`job[]`";
        modifiedField = modifiedField.replace("[", "`[")
                .substring(0, modifiedField.length());
        return modifiedField;

    }

    public static String addSpecialAvoidKeyWords(String express) {
        String REGEX = "\\w+"; //除特殊字符
        String DIGTALREGEX = "^[0-9]*$"; //匹配纯数字
        StringBuilder stringBuilder = new StringBuilder(express);
        Pattern p = Pattern.compile(REGEX);
        Matcher matcher = p.matcher(express);
        Pattern digatalPattern = Pattern.compile(DIGTALREGEX);
        Matcher digatalMatcher = null;
        int count = 0;
        while (matcher.find()) { //匹配含有特殊字段
            String word = stringBuilder.substring(matcher.start() + (count * 2), matcher.end() + (count * 2));
            digatalMatcher = digatalPattern.matcher(word);
            if (digatalMatcher.find()) {
                //如果是数字，则跳过
                continue;
            }
            stringBuilder.insert(matcher.start() + (count * 2), "`");
            stringBuilder.insert(matcher.end() + 1 + (count * 2), "`");
            count++;
        }
        return stringBuilder.toString();
    }
}
