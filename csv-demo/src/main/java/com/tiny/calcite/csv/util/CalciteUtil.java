package com.tiny.calcite.csv.util;

import com.tiny.calcite.csv.adapter.CsvCostFactory;
import com.tiny.calcite.csv.adapter.CsvDataTypeSystem;
import com.tiny.calcite.csv.adapter.CsvFunctionOperatorTable;
import com.tiny.calcite.csv.adapter.CsvSchemaFactory;
import com.tiny.calcite.csv.adapter.ExpressionReducer;
import com.tiny.calcite.csv.adapter.ViewExpanderImpl;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Properties;

/**
 * register table
 *
 * @author tiny.wang
 */
public class CalciteUtil {

    public static CalciteCatalogReader getCatalogReader(SchemaPlus rootSchema, RelDataTypeFactory typeFactory) {
        Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path(null),
                typeFactory,
                new CalciteConnectionConfigImpl(properties));
    }

    /***
     * TODO
     * what is the meaning of each config in FrameworkConfig ?
     */
    public static FrameworkConfig getFrameworkConfig(SchemaPlus rootSchema, SqlParser.Config parserConfig) {
        return Frameworks.newConfigBuilder()
                .context(Contexts.empty())
                .convertletTable(StandardConvertletTable.INSTANCE)
                .costFactory(CsvCostFactory.INSTANCE)
                .defaultSchema(rootSchema)
                .evolveLattice(false)
                .executor(new ExpressionReducer())
                .operatorTable(CalciteUtil.getSqlOperatorTable())
                .parserConfig(parserConfig)
                .sqlToRelConverterConfig(CalciteUtil.getSqlToRelConverterConfig())
                .viewExpander(new ViewExpanderImpl())
                .typeSystem(new CsvDataTypeSystem())
                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .build();
    }

    public static SqlParser.Config getSqlParserConfig() {
        return SqlParser.configBuilder()
                .setCaseSensitive(false)
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .setIdentifierMaxLength(SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH)
                .setLex(Lex.MYSQL)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();
    }

    public static SqlOperatorTable getSqlOperatorTable() {
        return ChainedSqlOperatorTable.of(SqlStdOperatorTable.instance(),
                new CsvFunctionOperatorTable());
    }

    public static SchemaPlus createRootSchema(Path path) {
        if (!Files.exists(path)) {
            throw new RuntimeException("path: " + path + "not found.");
        }
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        HashMap<String, Object> map = new HashMap<>(1);
        String fileName = path.getFileName().toString();
        map.put(fileName, path);
        CsvSchemaFactory.INSTANCE.create(rootSchema, fileName, map);
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

    public static SqlToRelConverter.Config getSqlToRelConverterConfig() {
        return SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .withInSubQueryThreshold(Integer.MAX_VALUE)
                .withExpand(false)
                .build();
    }

    public static RexBuilder createRexBuilder(RelDataTypeFactory typeFactory) {
        return new RexBuilder(typeFactory);
    }

}
