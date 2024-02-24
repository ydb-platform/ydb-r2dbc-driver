package tech.ydb.io.r2dbc.statement;


import io.r2dbc.spi.Statement;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.io.r2dbc.statement.binding.Binding;
import tech.ydb.io.r2dbc.statement.binding.BindingImpl;
import tech.ydb.io.r2dbc.statement.binding.Bindings;
import tech.ydb.io.r2dbc.statement.binding.BindingsImpl;
import tech.ydb.io.r2dbc.type.YdbTypeResolver;
import tech.ydb.io.r2dbc.query.YdbQuery;

/**
 * @author kuleshovegor
 */
public abstract class BaseStatement implements Statement {
    protected final YdbQuery query;
    protected final YdbConnectionState connectionState;

    protected final Bindings bindings;
    private final YdbTypeResolver ydbTypeResolver;

    public BaseStatement(YdbQuery query, YdbConnectionState connectionState, YdbTypeResolver ydbTypeResolver) {
        this.query = query;
        this.bindings = new BindingsImpl();
        this.ydbTypeResolver = ydbTypeResolver;
        this.connectionState = connectionState;
    }

    @Override
    public Statement add() {
        Binding binding = this.bindings.getCurrent();
        binding.validate();
        this.bindings.add(new BindingImpl());

        return this;
    }

    @Override
    public Statement bind(int index, Object object) {
        bindings.getCurrent().setParameter(index, object, ydbTypeResolver.toYdbType(object.getClass()));

        return this;
    }

    @Override
    public Statement bind(String name, Object object) {
        bindings.getCurrent().setParameter(name, object, ydbTypeResolver.toYdbType(object.getClass()));

        return this;
    }

    @Override
    public Statement bindNull(int index, Class<?> aClass) {
        bindings.getCurrent().setParameter(index, null, ydbTypeResolver.toYdbType(aClass));

        return this;
    }

    @Override
    public Statement bindNull(String name, Class<?> aClass) {
        bindings.getCurrent().setParameter(name, null, ydbTypeResolver.toYdbType(aClass));

        return this;
    }
}
