package com.tiny.calcite.csv.adapter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.util.Source;

import java.util.List;

/**
 * @author tiny.wang
 */
public class CsvFilterableTable extends CsvTable implements FilterableTable {

    public CsvFilterableTable(Source source, RelProtoDataType protoDataType) {
        super(source, protoDataType);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        return null;
    }
}
