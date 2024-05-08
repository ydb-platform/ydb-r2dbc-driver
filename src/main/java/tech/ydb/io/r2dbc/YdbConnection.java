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

package tech.ydb.io.r2dbc;

import com.google.common.annotations.VisibleForTesting;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;

import java.time.Duration;
import java.util.List;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.query.YdbSqlParser;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.io.r2dbc.state.NextStateResult;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.state.CloseState;
import tech.ydb.io.r2dbc.state.OutsideTransactionState;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.io.r2dbc.statement.YdbDMLStatement;
import tech.ydb.io.r2dbc.statement.YdbDDLStatement;
import tech.ydb.io.r2dbc.statement.YdbStatement;
import tech.ydb.table.query.Params;

/**
 * @author Egor Kuleshov
 */
public class YdbConnection implements Connection {
    private volatile YdbConnectionState ydbConnectionState;

    public YdbConnection(YdbContext ydbContext) {
        this.ydbConnectionState = new OutsideTransactionState(ydbContext, ydbContext.getDefaultYdbTxSettings());
    }

    public YdbConnection(YdbConnectionState ydbConnectionState) {
        this.ydbConnectionState = ydbConnectionState;
    }

    public Flux<YdbResult> executeDataQuery(String yql, Params params, List<OperationType> operationTypes) {
        return ydbConnectionState
                .executeDataQuery(yql, params, operationTypes)
                .doOnSuccess(fluxSessionResult -> updateState(fluxSessionResult.getNextState()))
                .flatMapMany(NextStateResult::getResult);
    }

    public Flux<YdbResult> executeSchemeQuery(String yql) {
        return ydbConnectionState.executeSchemeQuery(yql);
    }

    @Override
    public Mono<Void> beginTransaction() {
        final YdbConnectionState connectionState = ydbConnectionState;
        if (connectionState instanceof CloseState) {
            return Mono.error(new IllegalStateException(CloseState.CLOSED_STATE_MESSAGE));
        }

        return beginTransaction(connectionState, connectionState.getYdbTxSettings());
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        try {
            return beginTransaction(ydbConnectionState, new YdbTxSettings(definition));
        } catch (IllegalArgumentException exception) {
            return Mono.error(exception);
        }
    }

    private Mono<Void> beginTransaction(YdbConnectionState currentYdbConnectionState, YdbTxSettings ydbTxSettings) {
        return currentYdbConnectionState
                .beginTransaction(ydbTxSettings)
                .doOnSuccess(this::updateState)
                .then();
    }

    @Override
    public Mono<Void> close() {
        return ydbConnectionState
                .close()
                .doOnSuccess(unused -> updateState(CloseState.INSTANCE))
                .then();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return ydbConnectionState
                .commitTransaction()
                .doOnSuccess(this::updateState)
                .then();
    }

    @Override
    public Batch createBatch() {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported batch queries");
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported savepoint");
    }

    @Override
    public YdbStatement createStatement(String sql) {
        YdbQuery query = YdbSqlParser.parse(sql);

        return switch (query.type()) {
            case DML -> new YdbDMLStatement(query, this);
            case DDL -> new YdbDDLStatement(query, this);
        };
    }

    @Override
    public boolean isAutoCommit() {
        return ydbConnectionState.getYdbTxSettings().isAutoCommit();
    }

    @Override
    public YdbConnectionMetadata getMetadata() {
        return YdbConnectionMetadata.INSTANCE;
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return ydbConnectionState.getYdbTxSettings().getIsolationLevel();
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported savepoint");
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return ydbConnectionState
                .rollbackTransaction()
                .doOnSuccess(this::updateState)
                .then();
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported savepoint");
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return ydbConnectionState
                .setAutoCommit(autoCommit)
                .doOnSuccess(this::updateState)
                .then();
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration timeout) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported lock wait timeout");
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        return ydbConnectionState.setStatementTimeout(timeout);
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        return ydbConnectionState.setIsolationLevel(isolationLevel);
    }

    public boolean isReadOnly() {
        return ydbConnectionState.getYdbTxSettings().isReadOnly();
    }

    public Mono<Void> setReadOnly(boolean readOnly) {
        return ydbConnectionState.setReadOnly(readOnly);
    }

    @VisibleForTesting
    YdbConnectionState getCurrentState() {
        return ydbConnectionState;
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        return Mono.just(true);
    }

    private void updateState(YdbConnectionState ydbConnectionState) {
        this.ydbConnectionState = ydbConnectionState;
    }
}
