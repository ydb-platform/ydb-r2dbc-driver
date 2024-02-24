package tech.ydb.io.r2dbc.statement;

import io.r2dbc.spi.Statement;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.io.r2dbc.type.YdbTypeResolver;

/**
 * @author kuleshovegor
 */
public interface YdbStatementFactory {
    Statement createStatement(YdbQuery query, YdbConnectionState connectionState, YdbTypeResolver ydbTypeResolver);
}
