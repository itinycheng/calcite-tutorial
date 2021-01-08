package com.tiny.calcite.csv.adapter;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

/**
 * TODO
 *
 * @author tiny.wang
 */
public class CsvFunctionOperatorTable extends ReflectiveSqlOperatorTable {

    /**
     * The standard operator table.
     */
    private static CsvFunctionOperatorTable instance;

    public static synchronized CsvFunctionOperatorTable instance() {
        if (instance == null) {
            instance = new CsvFunctionOperatorTable();
            instance.init();
        }
        return instance;
    }

    public static final SqlFunction SQUARE =
            new SqlFunction(
                    "SQUARE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
        super.lookupOperatorOverloads(opName, category, syntax, operatorList, SqlNameMatchers.withCaseSensitive(false));
    }

}
