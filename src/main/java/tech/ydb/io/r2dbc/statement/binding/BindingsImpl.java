package tech.ydb.io.r2dbc.statement.binding;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author kuleshovegor
 */
public class BindingsImpl implements Bindings {
    @Override
    public Binding getCurrent() {
        return null;
    }

    @Override
    public void add(Binding binding) {

    }

    @Override
    public Iterator<Binding> iterator() {
        return null;
    }

    @Override
    public void forEach(Consumer<? super Binding> action) {
        Bindings.super.forEach(action);
    }

    @Override
    public Spliterator<Binding> spliterator() {
        return Bindings.super.spliterator();
    }
}
