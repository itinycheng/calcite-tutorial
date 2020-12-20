package com.tiny.calcite.csv.adapter;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.List;

/**
 * TODO
 *
 * @author tiny.wang
 */
public class CsvFunctionOperatorTable implements SqlOperatorTable {
    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {

    }

    @Override
    public List<SqlOperator> getOperatorList() {
        return null;
    }
}
