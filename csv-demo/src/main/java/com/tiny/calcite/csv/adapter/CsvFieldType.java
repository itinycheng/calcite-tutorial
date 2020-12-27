package com.tiny.calcite.csv.adapter;

import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author tiny.wang
 */
public enum CsvFieldType {
    /**
     * enums
     */
    STRING(String.class, "string"),
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.SHORT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.FLOAT),
    DOUBLE(Primitive.DOUBLE),
    DATE(java.sql.Date.class, "date"),
    TIME(java.sql.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private final static Map<String, CsvFieldType> ENUM_MAP = Arrays.stream(values())
            .collect(Collectors.toMap(value -> value.simpleName, value -> value));

    private final Class<?> clazz;

    private final String simpleName;

    CsvFieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveName);
    }

    CsvFieldType(Class<?> clazz, String simpleName) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(simpleName);
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    public static CsvFieldType of(String typeName){
        return ENUM_MAP.get(typeName);
    }

    public RelDataType toSqlType(RelDataTypeFactory typeFactory){
        RelDataType javaType = typeFactory.createJavaType(this.clazz);
        RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
        return typeFactory.createTypeWithNullability(sqlType,true);
    }
}
