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
import reactor.core.publisher.Flux;
import tech.ydb.io.r2dbc.QueryExecutor;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.statement.binding.Bindings;
import tech.ydb.io.r2dbc.query.YdbQuery;

/**
 * @author Egor Kuleshov
 */
public abstract class YdbStatement implements Statement {
    protected final YdbQuery query;
    protected final QueryExecutor queryExecutor;

    protected final Bindings bindings;

    public YdbStatement(YdbQuery query, QueryExecutor queryExecutor) {
        this.query = query;
        this.bindings = new Bindings(query.getIndexArgNames());
        this.queryExecutor = queryExecutor;
    }

    @Override
    public YdbStatement add() {
        bindings.add();

        return this;
    }

    @Override
    public YdbStatement bind(int index, Object object) {
        bindings.getCurrent().bind(index, object);

        return this;
    }

    @Override
    public YdbStatement bind(String name, Object object) {
        bindings.getCurrent().bind(name, object);

        return this;
    }

    @Override
    public YdbStatement bindNull(int index, Class<?> aClass) {
        bindings.getCurrent().bindNull(index, aClass);

        return this;
    }

    @Override
    public YdbStatement bindNull(String name, Class<?> aClass) {
        bindings.getCurrent().bindNull(name, aClass);

        return this;
    }

    @Override
    public abstract Flux<YdbResult> execute();

    Bindings getBindings() {
        return bindings;
    }
}
