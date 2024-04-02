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

package tech.ydb.io.r2dbc.state;

import java.time.Duration;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.result.YdbDDLResult;
import tech.ydb.io.r2dbc.result.YdbDMLResult;
import tech.ydb.table.query.Params;

/**
 * @author Egor Kuleshov
 */
public interface QueryExecutor {
    Flux<YdbDMLResult> executeDataQuery(String yql, Params params);

    Flux<YdbDDLResult> executeSchemaQuery(String yql);

    Mono<Void> beginTransaction();

    Mono<Void> beginTransaction(YdbTxSettings ydbTxSettings);

    Mono<Void> commitTransaction();

    Mono<Void> rollbackTransaction();

    Mono<Void> updateState(Function<YdbConnectionState, YdbConnectionState> function);

    YdbConnectionState getCurrentState();

    Mono<Void> setAutoCommit(boolean autoCommit);

    Mono<Void> close();

    Mono<Void> setStatementTimeout(Duration timeout);
}
