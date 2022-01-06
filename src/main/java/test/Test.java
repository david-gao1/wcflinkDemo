package test;


import org.apache.commons.collections.CollectionUtils;


/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/12/28 上午11:53
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) {
        if (!CollectionUtils.sizeIsEmpty(args)) {
            for (String arg : args) {
                System.out.println(arg);
            }
        }
        Test.class.getResourceAsStream("flinkSQL.demo/0001baisicCDC.sql");
    }
}
