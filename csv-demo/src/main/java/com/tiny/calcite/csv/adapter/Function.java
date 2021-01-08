package com.tiny.calcite.csv.adapter;

/**
 * @author tiny
 */
@FunctionalInterface
public interface Function<T, R> {

    R apply(T t) throws Exception;

}
