package com.tiny.calcite.csv.adapter;

import com.google.common.collect.Multimap;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author tiny
 */
public class CsvSchema extends AbstractSchema {

    private final String name;

    private final Path dirPath;

    private Map<String, Table> tables;

    public CsvSchema(String name, Path dirPath) {
        this.name = name;
        this.dirPath = dirPath;
        tables = new HashMap<>();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tables.size() == 0) {
            tables = loadTables();
        }
        return tables;
    }

    private Map<String, Table> loadTables() {
        return Util.silentException(Files::list, dirPath)
                .map(Path::toFile)
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().endsWith(".csv"))
                .collect(Collectors.toMap(file -> file.getName().replace(".csv", ""),
                        file -> newTable(Sources.of(file))));
    }

    private CsvTable newTable(Source source) {
        return new CsvTranslatableTable(source, null);
        // return new CsvFilterableTable(source, null);
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return super.getSubSchemaMap();
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return super.getFunctionMultimap();
    }

    @Override
    public boolean isMutable() {
        return super.isMutable();
    }
}
