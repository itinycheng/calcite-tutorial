package com.tiny.calcite.csv.util;

import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.HashMap;
import java.util.List;

/**
 * register table
 *
 * @author tiny.wang
 */
public class CalciteUtil {

    public static SchemaPlus registerRootSchema() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        HashMap<String, Object> map = new HashMap<>(1);
        map.put("directory", "/Users/tiny/Applications/IdeaProjects/calcite/example/csv/src/test/resources/bug");
        Schema schema = CsvSchemaFactory.INSTANCE.create(rootSchema, "BUG", map);
        //  protected, use getTableNames instead.
        // ((CsvSchema)schema).getTableMap()
        schema.getTableNames();
        rootSchema.add("CSV", schema);
        return rootSchema;
    }

    public static SqlConformance conformance(FrameworkConfig config) {
        final Context context = config.getContext();
        if (context != null) {
            final CalciteConnectionConfig connectionConfig =
                    context.unwrap(CalciteConnectionConfig.class);
            if (connectionConfig != null) {
                return connectionConfig.conformance();
            }
        }
        return SqlConformanceEnum.DEFAULT;
    }

    public static RexBuilder createRexBuilder(RelDataTypeFactory typeFactory) {
        return new RexBuilder(typeFactory);
    }

    public static class ViewExpanderImpl implements RelOptTable.ViewExpander {
        public ViewExpanderImpl() {
        }

        @Override
        public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
                                  List<String> viewPath) {
            return null;
        }
    }
}
