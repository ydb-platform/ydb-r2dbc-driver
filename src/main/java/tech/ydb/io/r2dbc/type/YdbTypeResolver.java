package tech.ydb.io.r2dbc.type;

import tech.ydb.table.values.Type;

/**
 * @author kuleshovegor
 */
public interface YdbTypeResolver {
    Type toYdbType(Class<?> type);
}
