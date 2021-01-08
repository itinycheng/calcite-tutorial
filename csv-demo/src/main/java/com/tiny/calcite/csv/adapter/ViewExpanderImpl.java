package com.tiny.calcite.csv.adapter;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * @author tiny.wang
 */
public class ViewExpanderImpl implements RelOptTable.ViewExpander {
    public ViewExpanderImpl() {
    }

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
                              List<String> viewPath) {
        return null;
    }
}