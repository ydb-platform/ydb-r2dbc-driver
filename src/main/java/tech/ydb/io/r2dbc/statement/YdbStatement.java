/*
 * Copyright 2022 YANDEX LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * @author Egor Kuleshov
 */
public abstract class YdbStatement implements Statement {
    protected final YdbQuery query;
    protected final YdbConnectionState connectionState;

    protected final Bindings bindings;

    public YdbStatement(YdbQuery query, YdbConnectionState connectionState) {
        this.query = query;
        this.bindings = new BindingsImpl();
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
        bindings.getCurrent().setParameter(index, object, YdbTypeResolver.toYdbType(object.getClass()));

        return this;
    }

    @Override
    public Statement bind(String name, Object object) {
        bindings.getCurrent().setParameter(name, object, YdbTypeResolver.toYdbType(object.getClass()));

        return this;
    }

    @Override
    public Statement bindNull(int index, Class<?> aClass) {
        bindings.getCurrent().setParameter(index, null, YdbTypeResolver.toYdbType(aClass));

        return this;
    }

    @Override
    public Statement bindNull(String name, Class<?> aClass) {
        bindings.getCurrent().setParameter(name, null, YdbTypeResolver.toYdbType(aClass));

        return this;
    }
}
