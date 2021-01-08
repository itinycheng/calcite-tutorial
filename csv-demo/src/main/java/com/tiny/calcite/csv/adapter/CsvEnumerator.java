package com.tiny.calcite.csv.adapter;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Source;

import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiny.wang
 */
public class CsvEnumerator<T> implements Enumerator<T> {

    private final String[] filterValues;

    private final AtomicBoolean cancelFlag;

    private final RowConverter<T> converter;

    private T current;

    private final LineNumberReader lineReader;

    public CsvEnumerator(Source source, String[] filterValues, AtomicBoolean cancelFlag, RowConverter<T> converter) {
        this.filterValues = filterValues;
        this.cancelFlag = cancelFlag;
        this.converter = converter;
        try {
            lineReader = new LineNumberReader(source.reader());
            lineReader.readLine(); // skip first line
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        outer:
        while (true) {
            if (cancelFlag.get()) {
                return false;
            }
            try {
                String line = lineReader.readLine();
                if (line != null) {
                    String[] row = line.split(",");
                    for (int i = 0; i < filterValues.length; i++) {
                        if (filterValues[i] != null
                                && filterValues[i].equals(row[i])) {
                            continue outer;
                        }
                    }
                    current = converter.convertRow(row);
                    return true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        }
    }

    @Override
    public void reset() {
        try {
            lineReader.reset();
            lineReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            lineReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static abstract class RowConverter<T> {

        /**
         * udf convert function
         *
         * @param row line
         * @return T return value
         */
        abstract T convertRow(String[] row);

        public Object convert(CsvFieldType fieldType, String value) {
            if (fieldType == null) {
                return value;
            }
            if (value.length() == 0) {
                return null;
            }
            switch (fieldType) {
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                case BYTE:
                    return Byte.parseByte(value);
                case CHAR:
                    return value.toCharArray()[0];
                case DATE:
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        return new Date(sdf.parse(value).getTime());
                    } catch (ParseException e) {
                        return null;
                    }
                case DOUBLE:
                    return Double.parseDouble(value);
                case FLOAT:
                    return Float.parseFloat(value);
                case INT:
                    return Integer.parseInt(value);
                case LONG:
                    return Long.parseLong(value);
                case SHORT:
                    return Short.parseShort(value);
                case TIME:
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        return new Time(sdf.parse(value).getTime());
                    } catch (ParseException e) {
                        return null;
                    }
                case TIMESTAMP:
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        return new Timestamp(sdf.parse(value).getTime());
                    } catch (ParseException e) {
                        return null;
                    }
                default:
                    return value;
            }
        }

    }

    static class ArrayRowConverter extends RowConverter<Object[]> {

        private final List<CsvFieldType> fieldTypes;

        public ArrayRowConverter(List<CsvFieldType> fieldTypes) {
            this.fieldTypes = fieldTypes;
        }

        @Override
        Object[] convertRow(String[] row) {
            int len = fieldTypes.size();
            Object[] objects = new Object[len];
            for (int i = 0; i < len; i++) {
                objects[i] = convert(fieldTypes.get(i), row[i]);
            }
            return objects;
        }
    }
}
