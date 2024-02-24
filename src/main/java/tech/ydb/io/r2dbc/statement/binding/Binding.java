package tech.ydb.io.r2dbc.statement.binding;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;

/**
 * @author kuleshovegor
 */
public interface Binding extends Params {
    void clearParameters();

    void setParameter(int index, @Nullable Object obj, @Nonnull Type type);
    void setParameter(String name, @Nullable Object obj, @Nonnull Type type);

    String getNameByIndex(int index);

    int parametersCount();

    void validate();
}
