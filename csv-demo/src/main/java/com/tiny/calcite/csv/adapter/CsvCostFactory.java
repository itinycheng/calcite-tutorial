package com.tiny.calcite.csv.adapter;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

/**
 * TODO
 *
 * @author tiny.wang
 */
public class CsvCostFactory implements RelOptCostFactory {

    public static final CsvCostFactory INSTANCE = new CsvCostFactory();

    @Override
    public RelOptCost makeCost(double rowCount, double cpu, double io) {
        return null;
    }

    @Override
    public RelOptCost makeHugeCost() {
        return null;
    }

    @Override
    public RelOptCost makeInfiniteCost() {
        return null;
    }

    @Override
    public RelOptCost makeTinyCost() {
        return null;
    }

    @Override
    public RelOptCost makeZeroCost() {
        return null;
    }
}
