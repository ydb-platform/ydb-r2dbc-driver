package tech.ydb.io.r2dbc.statement;

import java.sql.SQLException;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.io.r2dbc.type.YdbTypeResolver;

/**
 * @author kuleshovegor
 */
public class DataStatement extends BaseStatement {

    public DataStatement(YdbQuery query, YdbConnectionState ydbConnectionState, YdbTypeResolver ydbTypeResolver) {
        super(query, ydbConnectionState, ydbTypeResolver);
    }

    @Override
    public Statement bind(int i, Object o) {
        return null;
    }

    @Override
    public Statement bind(String s, Object o) {
        return null;
    }

    @Override
    public Statement bindNull(int i, Class<?> aClass) {
        return null;
    }

    @Override
    public Statement bindNull(String s, Class<?> aClass) {
        return null;
    }

    @Override
    public Publisher<? extends Result> execute() {
        try {
            return Flux.fromIterable(bindings)
                    .flatMap(binding -> {
                        try {
                            return connectionState.executeDataQuery(query.getYqlQuery(bindings.getCurrent()),
                                            bindings.getCurrent());
                        } catch (SQLException e) {
                            return Mono.error(e);
                        }
                    });
        } catch (Exception e) {
            return Mono.error(e);
        }
    }
}
