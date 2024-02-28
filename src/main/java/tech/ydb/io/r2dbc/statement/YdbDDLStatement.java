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
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.result.YdbDDLResult;
import tech.ydb.io.r2dbc.state.YdbConnectionState;

/**
 * @author Egor Kuleshov
 */
public class YdbDDLStatement extends YdbStatement {
    private static final String NOT_SUPPORTED_MESSAGE = "Operation not supported for YdbDDLStatement";

    public YdbDDLStatement(YdbQuery query, YdbConnectionState ydbConnectionState) {
        super(query, ydbConnectionState);
    }
    @Override
    public Statement add() {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Statement bind(int i, Object o) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Statement bind(String s, Object o) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Statement bindNull(int i, Class<?> aClass) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Statement bindNull(String s, Class<?> aClass) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Mono<YdbDDLResult> execute() {
        try {
            return connectionState.executeSchemaQuery(query.getYqlQuery(bindings.getCurrent()));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }
}
