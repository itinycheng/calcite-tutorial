package com.tiny.calcite.csv.adapter;

import org.apache.calcite.linq4j.tree.Primitive;

/**
 * @author tiny.wang
 */
public enum CsvFieldType {
    /**
     * short -> int
     * float -> double
     */
    STRING(String.class, "string"),
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.INT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.DOUBLE),
    DOUBLE(Primitive.DOUBLE),
    DATE(java.sql.Date.class, "date"),
    TIME(java.sql.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private Class<?> clazz;

    private String simpleName;

    CsvFieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveName);
    }

    CsvFieldType(Class<?> clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }
}
