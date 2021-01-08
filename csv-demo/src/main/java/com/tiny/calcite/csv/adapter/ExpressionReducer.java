package com.tiny.calcite.csv.adapter;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * TODO
 *
 * @author tiny.wang
 */
public class ExpressionReducer implements RexExecutor {
    @Override
    public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {

    }
}
