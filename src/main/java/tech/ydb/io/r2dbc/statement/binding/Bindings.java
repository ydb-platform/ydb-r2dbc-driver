package tech.ydb.io.r2dbc.statement.binding;

/**
 * @author kuleshovegor
 */
public interface Bindings extends Iterable<Binding> {
    Binding getCurrent();
    void add(Binding binding);
}
