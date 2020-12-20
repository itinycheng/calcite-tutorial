package com.tiny.calcite.csv.adapter;

import com.google.common.collect.Multimap;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tiny
 */
public class CsvSchema extends AbstractSchema {

    private final String name;

    private final Path dirPath;

    private final Map<String, Table> tables;

    public CsvSchema(String name, Path dirPath) {
        this.name = name;
        this.dirPath = dirPath;
        tables = new HashMap<>();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tables.size() == 0) {
            loadTables();
        }
        return tables;
    }

    private void loadTables() {
        Util.silentException(Files::list, dirPath)
                .filter(path -> !path.toFile().isDirectory())
                .forEach(path -> {
                    String name = path.getFileName().toString();
                    Util.silentException(Files::lines, path)
                            .limit(1)
                            .forEach(s -> {
                                s.split(",");
                            });

                });

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
