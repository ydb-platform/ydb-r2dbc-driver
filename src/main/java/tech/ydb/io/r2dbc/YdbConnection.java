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

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;

import java.time.Duration;

import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.query.YdbSqlParser;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.OutsideTransactionState;
import tech.ydb.io.r2dbc.executor.QueryExecutor;
import tech.ydb.io.r2dbc.executor.QueryExecutorImpl;
import tech.ydb.io.r2dbc.statement.YdbDMLStatement;
import tech.ydb.io.r2dbc.statement.YdbDDLStatement;
import tech.ydb.io.r2dbc.statement.YdbStatement;

/**
 * @author Kirill Kurdyukov
 */
public class YdbConnection implements Connection {
    private final QueryExecutor queryExecutor;

    public YdbConnection(YdbContext ydbContext) {
        this.queryExecutor = new QueryExecutorImpl(ydbContext,
                new OutsideTransactionState(ydbContext, ydbContext.getDefaultYdbTxSettings()));
    }

    @Override
    public Mono<Void> beginTransaction() {
        return queryExecutor.beginTransaction();
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        try {
            return queryExecutor.beginTransaction(new YdbTxSettings(definition));
        } catch (IllegalArgumentException exception) {
            return Mono.error(exception);
        }
    }

    @Override
    public Mono<Void> close() {
        return queryExecutor.close();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return queryExecutor.commitTransaction();
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
            case DML -> new YdbDMLStatement(query, queryExecutor);
            case DDL -> new YdbDDLStatement(query, queryExecutor);
        };
    }

    @Override
    public boolean isAutoCommit() {
        return queryExecutor.getCurrentState().getYdbTxSettings().isAutoCommit();
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return null;
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        throw new UnsupportedOperationException(
                "Standard isolation levels not supported, use getYdbTransactionIsolationLevel");
    }

    public YdbIsolationLevel getYdbTransactionIsolationLevel() {
        return queryExecutor.getCurrentState().getYdbTxSettings().getIsolationLevel();
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported savepoint");
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return queryExecutor.rollbackTransaction();
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported savepoint");
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return queryExecutor.setAutoCommit(autoCommit);
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration timeout) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported lock wait timeout");
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        return queryExecutor.setStatementTimeout(timeout);
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        throw new UnsupportedOperationException(
                "Standard isolation levels not supported, use setYdbTransactionIsolationLevel");
    }

    public Mono<Void> setYdbTransactionIsolationLevel(YdbIsolationLevel isolationLevel) {
        return queryExecutor.updateState(ydbTxState -> ydbTxState.withIsolationLevel(isolationLevel));
    }

    public boolean isReadOnly() {
        return queryExecutor.getCurrentState().getYdbTxSettings().isReadOnly();
    }

    public Mono<Void> setReadOnly(boolean readOnly) {
        return queryExecutor.updateState(ydbTxState -> ydbTxState.withReadOnly(readOnly));
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        return Mono.just(true);
    }
}
