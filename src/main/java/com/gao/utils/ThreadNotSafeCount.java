package com.gao.utils;

/**
 * @Description TODO
 * @Author lianggao
 * @Date 2021/11/24 上午10:24
 * @Version 1.0
 */
public class ThreadNotSafeCount {
    private Long value;


    public synchronized void inc() {
        ++value;
    }

    public synchronized Long getCount() {
        return value;
    }
}
