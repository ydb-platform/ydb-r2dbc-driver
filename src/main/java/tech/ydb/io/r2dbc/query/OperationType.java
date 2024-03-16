package tech.ydb.io.r2dbc.query;

/**
 * @author kuleshovegor
 */
public enum OperationType {
    SELECT(QueryType.DML),
    UPDATE(QueryType.DML),
    SCHEME(QueryType.DDL);

    private final QueryType queryType;

    OperationType(QueryType queryType) {
        this.queryType = queryType;
    }

    public QueryType getQueryType() {
        return queryType;
    }
}
