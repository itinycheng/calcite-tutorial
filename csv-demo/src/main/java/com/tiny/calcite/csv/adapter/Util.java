package com.tiny.calcite.csv.adapter;

/**
 * @author tiny.wang
 */
public class Util {

    public static <T, R> R silentException(Function<T, R> function, T t) {
        try {
            return function.apply(t);
        } catch (Exception e) {
            throw new RuntimeException("exec failed.", e);
        }
    }
}
