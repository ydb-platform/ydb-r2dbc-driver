package tech.ydb.io.r2dbc.statement;

import javax.naming.OperationNotSupportedException;

import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.io.r2dbc.type.YdbTypeResolver;

/**
 * @author kuleshovegor
 */
public class ScanStatement extends BaseStatement {
    public ScanStatement(YdbQuery query, YdbConnectionState connectionState, YdbTypeResolver ydbTypeResolver) {
        super(query, connectionState, ydbTypeResolver);
    }

    @Override
    public Publisher<? extends Result> execute() {
        return Mono.error(new OperationNotSupportedException("scan not supported now"));
    }
}
