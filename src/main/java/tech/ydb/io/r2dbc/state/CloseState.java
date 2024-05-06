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
import java.util.List;

import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.table.query.Params;

/**
 * Implementation state of the closed connection.
 * Throw or return an {@link IllegalStateException} on the call of each method
 *
 * @author Egor Kuleshov
 */
public class CloseState implements YdbConnectionState {
    public static final String CLOSED_STATE_MESSAGE = "Connection closed";
    public static final CloseState INSTANCE = new CloseState();

    private CloseState() {
    }

    @Override
    public Mono<NextStateResult<Flux<YdbResult>>> executeDataQuery(String yql, Params params, List<OperationType> operationTypes) {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public Flux<YdbResult> executeSchemeQuery(String yql) {
        return Flux.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public Mono<InsideTransactionState> beginTransaction(YdbTxSettings ydbTxSettings) {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public Mono<OutsideTransactionState> commitTransaction() {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public Mono<OutsideTransactionState> rollbackTransaction() {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public Mono<YdbConnectionState> setAutoCommit(boolean autoCommit) {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public Mono<Void> setIsolationLevel(IsolationLevel isolationLevel) {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public Mono<Void> setReadOnly(boolean readOnly) {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public YdbTxSettings getYdbTxSettings() {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public Mono<Void> close() {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }
}
