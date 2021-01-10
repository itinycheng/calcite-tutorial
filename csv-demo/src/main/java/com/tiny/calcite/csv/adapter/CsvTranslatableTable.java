package com.tiny.calcite.csv.adapter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Source;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiny.wang
 */
public class CsvTranslatableTable extends CsvTable implements QueryableTable, TranslatableTable {

    public CsvTranslatableTable(Source source, RelProtoDataType protoDataType) {
        super(source, protoDataType);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        int fieldCount = relOptTable.getRowType().getFieldCount();
        int[] fields = CsvEnumerator.identityList(fieldCount);
        return new CsvTableScan(context.getCluster(), relOptTable, this, fields);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, this.getElementType(), tableName, clazz);
    }

    public Enumerable<Object[]> project(DataContext root, int[] fields) {
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(CsvTranslatableTable.this.source,
                        new String[0],
                        cancelFlag,
                        new CsvEnumerator.ArrayRowConverter(new ArrayList<>(fieldTypeMap.values()), fields));
            }
        };
    }
}
