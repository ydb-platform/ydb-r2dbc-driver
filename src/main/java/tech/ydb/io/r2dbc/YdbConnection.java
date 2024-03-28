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
import tech.ydb.io.r2dbc.state.OutTransaction;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.io.r2dbc.statement.YdbDMLStatement;
import tech.ydb.io.r2dbc.statement.YdbDDLStatement;
import tech.ydb.io.r2dbc.statement.YdbStatement;
import tech.ydb.table.TableClient;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Kirill Kurdyukov
 */
public class YdbConnection implements Connection {
    private volatile IsolationLevel isolationLevel = IsolationLevel.SERIALIZABLE;
    private volatile boolean autoCommit = true;
    private volatile YdbConnectionState state;

    public YdbConnection(TableClient tableClient) {
        this.state = new OutTransaction(tableClient, TxControl.serializableRw(), Duration.ofSeconds(1));
    }

    @Override
    public Mono<Void> beginTransaction() {
        return null;
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        return null;
    }

    @Override
    public Mono<Void> close() {
        return Mono.empty();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return null;
    }

    @Override
    public Batch createBatch() {
        return new YdbBatch(state);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported savepoint");
    }

    @Override
    public YdbStatement createStatement(String sql) {
        YdbQuery query =  YdbSqlParser.parse(sql);

        return switch (query.type()) {
            case DML -> new YdbDMLStatement(query, state);
            case DDL -> new YdbDDLStatement(query, state);
        };
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return null;
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported savepoint");
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return null;
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException("YDB R2DBC driver is unsupported savepoint");
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;

        return Mono.empty();
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration timeout) {
        return null;
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        return null;
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
        return Mono.empty();
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        return Mono.just(true);
    }
}
