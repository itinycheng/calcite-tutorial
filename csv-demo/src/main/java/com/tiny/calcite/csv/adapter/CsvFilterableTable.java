package com.tiny.calcite.csv.adapter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiny.wang
 */
public class CsvFilterableTable extends CsvTable implements FilterableTable {

    public CsvFilterableTable(Source source, RelProtoDataType protoDataType) {
        super(source, protoDataType);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        String[] filterValues = new String[fieldTypeMap.size()];
        filters.removeIf(filter -> addFilter(filter, filterValues));
        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(source, filterValues, cancelFlag,
                        new CsvEnumerator.ArrayRowConverter(new ArrayList<>(fieldTypeMap.values())));
            }
        };
    }

    private boolean addFilter(RexNode filter, String[] filterValues) {
        if (filter.isA(SqlKind.AND)) {
            ((RexCall) filter).getOperands().forEach(subFilter -> addFilter(subFilter, filterValues));
        } else if (filter.isA(SqlKind.EQUALS)) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).getOperands().get(0);
            }
            final RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                final int index = ((RexInputRef) left).getIndex();
                if (filterValues[index] == null) {
                    filterValues[index] = ((RexLiteral) right).getValue2().toString();
                    return true;
                }
            }
        }
        return false;
    }

}
