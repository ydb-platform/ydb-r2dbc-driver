package tech.ydb.io.r2dbc.query;

/**
 * @author kuleshovegor
 */
public interface R2dbcQueryParser {
    YdbQuery parseYdbQuery(String sql);
}
