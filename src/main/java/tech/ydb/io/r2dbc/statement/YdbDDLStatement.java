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

import reactor.core.publisher.Flux;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.state.QueryExecutor;
import tech.ydb.io.r2dbc.statement.binding.Binding;

/**
 * @author Egor Kuleshov
 */
public class YdbDDLStatement extends YdbStatement {
    private static final String NOT_SUPPORTED_MESSAGE = "Operation not supported for YdbDDLStatement";

    public YdbDDLStatement(YdbQuery query, QueryExecutor queryExecutor) {
        super(query, queryExecutor);
    }

    @Override
    public YdbStatement add() {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public YdbStatement bind(int i, Object o) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public YdbStatement bind(String s, Object o) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public YdbStatement bindNull(int i, Class<?> aClass) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public YdbStatement bindNull(String s, Class<?> aClass) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Flux<YdbResult> execute() {
        try {
            return queryExecutor.executeSchemaQuery(query.getYqlQuery(Binding.empty()));
        } catch (Exception e) {
            return Flux.error(e);
        }
    }
}
