package com.tiny.calcite.csv.adapter;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author tiny.wang
 */
public class CsvSchemaFactory implements SchemaFactory {

    public static final CsvSchemaFactory INSTANCE = new CsvSchemaFactory();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        operand.forEach((dirName, path) -> {
            CsvSchema csvSchema = new CsvSchema(dirName, (Path) path);
            SchemaPlus schemaPlus = parentSchema.add(dirName, csvSchema);
            try (Stream<Path> stream = Files.list((Path) path)) {
                Map<String, Object> subDirs = stream.filter(Files::isDirectory)
                        .collect(Collectors.toMap(dir -> dir.getFileName().toString(),
                                dir -> dir));
                if (subDirs.size() > 0) {
                    create(schemaPlus, null, subDirs);
                }
            } catch (Exception e) {
                throw new RuntimeException("parse schema failed");
            }
        });

        return name != null ?
                parentSchema.getSubSchema(name)
                : null;
    }
}
