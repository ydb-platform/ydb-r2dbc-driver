package tech.ydb.io.r2dbc.query;

/**
 * @author kuleshovegor
 */
public enum ExpressionType {
    SELECT(QueryType.DML),
    UPDATE(QueryType.DML),
    SCHEME(QueryType.DDL);

    private final QueryType queryType;

    ExpressionType(QueryType queryType) {
        this.queryType = queryType;
    }

    public QueryType getQueryType() {
        return queryType;
    }
}
