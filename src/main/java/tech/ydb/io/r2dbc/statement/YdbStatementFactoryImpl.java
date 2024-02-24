package tech.ydb.io.r2dbc.statement;

import io.r2dbc.spi.Statement;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.io.r2dbc.type.YdbTypeResolver;

/**
 * @author kuleshovegor
 */
public class YdbStatementFactoryImpl implements YdbStatementFactory {
    @Override
    public Statement createStatement(YdbQuery query, YdbConnectionState connectionState, YdbTypeResolver ydbTypeResolver) {
        return switch (query.type()) {
            case DATA_QUERY -> new DataStatement(query, connectionState, ydbTypeResolver);
            case SCHEME_QUERY -> new SchemaStatement(query, connectionState, ydbTypeResolver);

            case SCAN_QUERY -> new ScanStatement(query, connectionState, ydbTypeResolver);
        };
    }
}
