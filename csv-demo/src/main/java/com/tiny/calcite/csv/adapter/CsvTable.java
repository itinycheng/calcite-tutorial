package com.tiny.calcite.csv.adapter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author tiny.wang
 */
public class CsvTable extends AbstractTable {

    protected final Source source;

    protected final RelProtoDataType protoDataType;

    protected Map<String, CsvFieldType> fieldTypeMap;

    public CsvTable(Source source, RelProtoDataType protoDataType) {
        this.source = source;
        this.protoDataType = protoDataType;
    }

    /**
     * return row sql type
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoDataType != null) {
            return protoDataType.apply(typeFactory);
        } else if (fieldTypeMap != null) {
            return deduceRowType(source, typeFactory, null);
        } else {
            fieldTypeMap = new LinkedHashMap<>(16);
            return deduceRowType(source, typeFactory, fieldTypeMap);
        }
    }

    private RelDataType deduceRowType(Source source, RelDataTypeFactory typeFactory, Map<String, CsvFieldType> fieldTypeMap) {
        String header = Util.silentException(Files::lines, source.file().toPath())
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("header not found"));
        String[] columns = header.split(",");
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        for (String column : columns) {
            String[] arr = column.split(":");
            String name = arr[0];
            String typeString = arr.length == 2 ? arr[1] : null;
            CsvFieldType csvType;
            if (typeString == null) {
                csvType = CsvFieldType.STRING;
            } else {
                csvType = CsvFieldType.of(arr[1]);
            }
            if (name == null || name.isEmpty() || csvType == null) {
                throw new RuntimeException("unknown column name or type");
            }
            if (fieldTypeMap != null) {
                fieldTypeMap.put(name, csvType);
            }
            names.add(name);
            types.add(csvType.toSqlType(typeFactory));
        }
        return typeFactory.createStructType(types, names);
    }

}
