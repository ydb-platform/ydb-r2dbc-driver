package tech.ydb.io.r2dbc.query;

/**
 * @author kuleshovegor
 */
public enum QueryType {
    // DDL
    SCHEME_QUERY,

    // DML
    DATA_QUERY,
    SCAN_QUERY
}
