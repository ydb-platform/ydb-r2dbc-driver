package tech.ydb.io.r2dbc.statement;


import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.io.r2dbc.type.YdbTypeResolver;

/**
 * @author kuleshovegor
 */
public class SchemaStatement extends BaseStatement {
    public SchemaStatement(YdbQuery query, YdbConnectionState ydbConnectionState, YdbTypeResolver ydbTypeResolver) {
        super(query, ydbConnectionState, ydbTypeResolver);
    }
    @Override
    public Statement add() {
        return this;
    }

    @Override
    public Statement bind(int i, Object o) {
        return this;
    }

    @Override
    public Statement bind(String s, Object o) {
        return this;
    }

    @Override
    public Statement bindNull(int i, Class<?> aClass) {
        return this;
    }

    @Override
    public Statement bindNull(String s, Class<?> aClass) {
        return this;
    }

    @Override
    public Publisher<? extends Result> execute() {
        try {
            return connectionState.executeSchemaQuery(query.getYqlQuery(bindings.getCurrent()), bindings.getCurrent());
        } catch (Exception e) {
            return Mono.error(e);
        }
    }
}
