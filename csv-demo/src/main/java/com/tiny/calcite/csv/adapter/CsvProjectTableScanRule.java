package com.tiny.calcite.csv.adapter;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

/**
 * @author tiny.wang
 */
public class CsvProjectTableScanRule extends RelOptRule {

    public static final CsvProjectTableScanRule INSTANCE = new CsvProjectTableScanRule(RelFactories.LOGICAL_BUILDER);

    public CsvProjectTableScanRule(RelBuilderFactory relBuilderFactory) {
        super(operand(LogicalProject.class, operand(CsvTableScan.class, any())),
                relBuilderFactory,
                "CsvProjectTableScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        CsvTableScan tableScan = call.rel(1);
        int[] projectFields = this.getProjectFields(project.getProjects());
        if (projectFields != null) {
            call.transformTo(new CsvTableScan(tableScan.getCluster(),
                    tableScan.getTable(), tableScan.csvTable, projectFields));
        }
    }

    private int[] getProjectFields(List<RexNode> rexNodes) {
        int[] fields = new int[rexNodes.size()];
        for (int i = 0; i < rexNodes.size(); i++) {
            RexNode rexNode = rexNodes.get(i);
            if (!(rexNode instanceof RexInputRef)) {
                return null;
            }
            fields[i] = ((RexInputRef) rexNodes.get(i)).getIndex();
        }
        return fields;
    }
}
