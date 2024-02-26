package tech.ydb.io.r2dbc.query;


import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import tech.ydb.io.r2dbc.context.YdbConst;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Value;

/**
 * @author kuleshovegor
 */
public class YdbQuery {
    private final String yqlQuery;
    private final QueryType type;
    private final List<String> indexesArgsNames;

    YdbQuery(String yql, List<String> indexesArgsNames, QueryType queryType) {
        this.yqlQuery = yql;
        this.indexesArgsNames = indexesArgsNames;
        this.type = queryType;
    }

    public String getYqlQuery(Params params) throws SQLException {
        StringBuilder yql = new StringBuilder();

        if (indexesArgsNames != null) {
            if (params != null) {
                Map<String, Value<?>> values = params.values();
                for (String prm : indexesArgsNames) {
                    if (!values.containsKey(prm)) {
                        throw new SQLDataException(YdbConst.MISSING_VALUE_FOR_PARAMETER + prm);
                    }

                    String prmType = values.get(prm).getType().toString();
                    yql.append("DECLARE ")
                            .append(prm)
                            .append(" AS ")
                            .append(prmType)
                            .append(";\n");


                }
            } else if (!indexesArgsNames.isEmpty()) {
                // Comment in place where must be declare section
                yql.append("-- DECLARE ").append(indexesArgsNames.size()).append(" PARAMETERS").append("\n");
            }
        }

        yql.append(yqlQuery);
        return yql.toString();
    }

    public QueryType type() {
        return type;
    }
}
