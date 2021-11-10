package org.apache.flink.connector.jdbc.dialect;

import java.util.UUID;

/**
 * @Description TODO
 * @Author roman.gao
 * @Date 2021/9/16 7:41 下午
 */
public class TestUUID {
    public static void main(String[] args) {

        UUID uuid = UUID.randomUUID();
        uuid.toString();
        if (uuid instanceof UUID) {
            String s = uuid.toString();
            System.out.println(s);

        }
    }
}
