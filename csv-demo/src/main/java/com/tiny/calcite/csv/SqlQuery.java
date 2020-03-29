package com.tiny.calcite.csv;

import com.tiny.calcite.csv.util.CalciteUtil;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * query example, use VolcanoPlanner and HepPlanner
 * reference: git@github.com:yuqi1129/calcite-test.git
 *
 * @author tiny.wang
 */
@SuppressWarnings("unchecked")
public class SqlQuery {

    public static void main(String[] args) throws Exception {
        SchemaPlus rootSchema = CalciteUtil.registerRootSchema();

        final FrameworkConfig fromworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema)
                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .build();

        String sql = "select EMPNO, JOINTIME from CSV.\"DATE\" where EMPNO <= 160";
        // "select T1.EMPNO from CSV.\"DATE\" AS T1, CSV.\"LONG_EMPS\" AS T2 "
        // + "where T1.EMPNO = T2.EMPNO AND T1.EMPNO <= 160";

        // use VolcanoPlanner
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        // add rules
        RelOptUtil.registerDefaultRules(planner, true, false);

        JavaTypeFactoryImpl factory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        // sql parser
        SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
        SqlNode parsed = parser.parseStmt();
        System.out.println("-----------------------------------------------------------");
        System.out.println("The SqlNode after parsed is:\n " + parsed.toString());

        CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path(null),
                factory,
                new CalciteConnectionConfigImpl(new Properties()));

        // sql validate
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader,
                factory, CalciteUtil.conformance(fromworkConfig));
        SqlNode validated = validator.validate(parsed);
        System.out.println("-----------------------------------------------------------");
        System.out.println("The SqlNode after validated is:\n " + validated.toString());

        final RexBuilder rexBuilder = CalciteUtil.createRexBuilder(factory);
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // init SqlToRelConverter config
        final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                .withConfig(fromworkConfig.getSqlToRelConverterConfig())
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .build();

        // SqlNode toRelNode
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new CalciteUtil.ViewExpanderImpl(),
                validator, calciteCatalogReader, cluster, fromworkConfig.getConvertletTable(), config);
        RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);

        //tiny note: replace LogicalTableScan to CsvTableScan, impl this by invoke CsvTranslatableTable.toRel
        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        RelNode relNode = root.rel;
        System.out.println("-----------------------------------------------------------");
        System.out.println("The relational expression string before optimized is:\n " + RelOptUtil.toString(relNode));

        // replace and cache TraitSet
        RelTraitSet desiredTraits = relNode.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
        // create RelSet & RelSubset and init Cost, wrapped RelNode with RelSet/SubRelSet, put desiredTraits to outside RelSubSet
        // put matched Rules to volcanoPlanner.ruleQueue, it will be used for relNode optimize
        relNode = planner.changeTraits(relNode, desiredTraits);

        planner.setRoot(relNode);
        relNode = planner.findBestExp();
        System.out.println("-----------------------------------------------------------");
        System.out.println("The Best relational expression string:\n " + RelOptUtil.toString(relNode));

        final HepProgramBuilder builder = HepProgram.builder();
        for (RelOptRule rule : RelOptRules.CALC_RULES) {
            builder.addRuleInstance(rule);
        }
        HepPlanner hepPlanner = new HepPlanner(builder.build(),
                null, true, null, RelOptCostImpl.FACTORY);
        List<RelMetadataProvider> objects = new ArrayList<>();
        objects.add(DefaultRelMetadataProvider.INSTANCE);
        hepPlanner.registerMetadataProviders(objects);
        hepPlanner.setRoot(relNode);
        relNode = hepPlanner.findBestExp();
        System.out.println("-----------------------------------------------------------");
        System.out.println("The Best relational expression string optimized by HepPlanner:\n "
                + RelOptUtil.toString(relNode));

        EnumerableRel enumerable = (EnumerableRel) relNode;
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("_conformance", SqlConformanceEnum.DEFAULT);
        parameters.put(DataContext.Variable.CANCEL_FLAG.camelName, new AtomicBoolean());
        // tiny note: call implement method in enumerable node to generate code.
        // in other words, each RelNode's implement method must be implemented.
        Bindable bindable = EnumerableInterpretable.toBindable(parameters,
                null, enumerable, EnumerableRel.Prefer.ARRAY);

        Enumerable enumerator = bindable.bind(new MyDataContext(parameters, rootSchema));
        Enumerable<Object[]> iterable = EnumerableDefaults.take(enumerator, 3);
        iterable.forEach(o -> System.out.println(o[0]));

    }

    private static class MyDataContext implements DataContext {
        private final Map<String, Object> parameters;

        private final SchemaPlus rootSchema;

        MyDataContext(Map<String, Object> parameters, SchemaPlus rootSchema) {
            this.parameters = parameters;
            this.rootSchema = rootSchema;
        }

        @Override
        public SchemaPlus getRootSchema() {
            return rootSchema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(String name) {
            return parameters.get(name);
        }
    }

    //===========bindable===========
    /*if (resultConvention == BindableConvention.INSTANCE) {
        bindable = Interpreters.bindable(root.rel);
        bindable.bind(context);
    }*/
}
