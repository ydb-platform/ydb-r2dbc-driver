package tech.ydb.io.r2dbc.statement.binding;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ydb.proto.ValueProtos;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * @author kuleshovegor
 */
public class BindingImpl implements Binding {
    @Override
    public void clearParameters() {

    }

    @Override
    public void setParameter(int index, @Nullable Object obj, @Nonnull Type type) {

    }

    @Override
    public void setParameter(String name, @Nullable Object obj, @Nonnull Type type) {

    }

    @Override
    public String getNameByIndex(int index) {
        return null;
    }

    @Override
    public int parametersCount() {
        return 0;
    }

    @Override
    public void validate() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public <T extends Type> Params put(String s, Value<T> value) {
        return null;
    }

    @Override
    public Map<String, ValueProtos.TypedValue> toPb() {
        return null;
    }

    @Override
    public Map<String, Value<?>> values() {
        return null;
    }
}
