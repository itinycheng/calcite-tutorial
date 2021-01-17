package com.tiny.calcite.csv.adapter;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.Objects;

/**
 * @author tiny.wang
 */
public class CsvTableScan extends TableScan implements EnumerableRel {

    final CsvTranslatableTable csvTable;

    final int[] fields;

    protected CsvTableScan(RelOptCluster cluster, RelOptTable table, CsvTranslatableTable csvTable, int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
        Objects.requireNonNull(csvTable);
        this.csvTable = csvTable;
        this.fields = fields;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new CsvTableScan(this.getCluster(), this.table, this.csvTable, this.fields);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fields", Primitive.asList(this.fields));
    }

    @Override
    public RelDataType deriveRowType() {
        List<RelDataTypeField> fieldList = this.table.getRowType().getFieldList();
        Builder builder = this.getCluster().getTypeFactory().builder();

        for (int field : this.fields) {
            builder.add(fieldList.get(field));
        }

        return builder.build();
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(CsvProjectTableScanRule.INSTANCE);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq)
                .multiplyBy(((double) this.fields.length + 2.0D)
                        / ((double) this.table.getRowType().getFieldCount() + 2.0D));
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), this.getRowType(), pref.preferArray());
        return implementor.result(physType, Blocks.toBlock(
                Expressions.call(this.table.getExpression(CsvTranslatableTable.class),
                        "project",
                        implementor.getRootExpression(),
                        Expressions.constant(this.fields))));

    }
}
