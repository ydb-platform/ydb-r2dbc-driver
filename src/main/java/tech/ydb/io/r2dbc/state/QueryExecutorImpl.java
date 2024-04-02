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
import tech.ydb.core.Result;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.result.YdbDDLResult;
import tech.ydb.io.r2dbc.result.YdbDMLResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.BeginTxSettings;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.ExecuteSchemeQuerySettings;
import tech.ydb.table.settings.RequestSettings;
import tech.ydb.table.settings.RollbackTxSettings;

/**
 * @author Egor Kuleshov
 */
public class QueryExecutorImpl implements QueryExecutor {
    private static final String SCHEME_QUERY_INSIDE_TRANSACTION = "Scheme query cannot be executed inside active "
            + "transaction. This behavior may be changed by property schemeQueryTxMode";
    private volatile YdbConnectionState ydbConnectionState;
    private volatile Duration statementTimeout;

    public QueryExecutorImpl(YdbContext ydbContext, YdbConnectionState ydbConnectionState) {
        this.ydbConnectionState = ydbConnectionState;
        this.statementTimeout = ydbContext.getDefaultTimeout();
    }

    @Override
    public Flux<YdbDMLResult> executeDataQuery(String yql, Params params) {
        return Flux.defer(() -> {
            final YdbConnectionState currentYdbConnectionState = ydbConnectionState;

            if (currentYdbConnectionState.isInTransaction()) {
                return Flux.error(new IllegalStateException(SCHEME_QUERY_INSIDE_TRANSACTION));
            }

            return currentYdbConnectionState.getSession().flatMap(session -> Mono.fromFuture(session.executeDataQuery(yql, currentYdbConnectionState.txControl(),
                            params, withStatementTimeout(new ExecuteDataQuerySettings()))))
                    .map(dataQueryResult -> new YdbDMLResult(dataQueryResult.getValue()))
                    .flux();
        });
    }

    @Override
    public Flux<YdbDDLResult> executeSchemaQuery(String yql) {
        return Flux.defer(() -> {
            final YdbConnectionState currentYdbConnectionState = ydbConnectionState;

            if (currentYdbConnectionState.isInTransaction()) {
                return Flux.error(new IllegalStateException(SCHEME_QUERY_INSIDE_TRANSACTION));
            }

            return currentYdbConnectionState.getSession()
                    .flatMap(session -> Mono.fromFuture(
                            session.executeSchemeQuery(yql,
                                    withStatementTimeout(new ExecuteSchemeQuerySettings()))))
                    .map(YdbDDLResult::new)
                    .flux();
        });
    }

    @Override
    public Mono<Void> beginTransaction() {
        return beginTransaction(ydbConnectionState, ydbConnectionState.getYdbTxSettings());
    }

    @Override
    public Mono<Void> beginTransaction(YdbTxSettings ydbTxSettings) {
        return beginTransaction(ydbConnectionState, ydbTxSettings);
    }

    private Mono<Void> beginTransaction(YdbConnectionState currentYdbConnectionState, YdbTxSettings ydbTxSettings) {
        if (currentYdbConnectionState.isInTransaction()) {
            return Mono.empty();
        }

        return currentYdbConnectionState.getSession()
                .flatMap(session -> Mono.fromFuture(session.beginTransaction(
                                ydbTxSettings.getMode(),
                                withStatementTimeout(new BeginTxSettings())))
                        .map(Result::getValue)
                        .doOnSuccess(transaction -> updateState(currentYdbConnectionState.withBeginTransaction(
                                transaction.getId(),
                                session,
                                ydbTxSettings))))
                .then();
    }

    @Override
    public Mono<Void> commitTransaction() {
        final YdbConnectionState currentYdbConnectionState = ydbConnectionState;

        if (!currentYdbConnectionState.isInTransaction()) {
            return Mono.empty();
        }
        final InsideTransactionState insideTransactionState = (InsideTransactionState) ydbConnectionState;

        return currentYdbConnectionState.getSession()
                .flatMap(session -> Mono.fromFuture(session.commitTransaction(
                                insideTransactionState.getId(),
                                withStatementTimeout(new CommitTxSettings())))
                        .doOnSuccess(unused -> updateState(currentYdbConnectionState.withCommitTransaction())))
                .then();
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        final YdbConnectionState currentYdbConnectionState = ydbConnectionState;

        if (!currentYdbConnectionState.isInTransaction()) {
            return Mono.empty();
        }
        final InsideTransactionState insideTransactionState = (InsideTransactionState) ydbConnectionState;

        return currentYdbConnectionState.getSession()
                .flatMap(session -> Mono.fromFuture(session.rollbackTransaction(
                                insideTransactionState.getId(),
                                withStatementTimeout(new RollbackTxSettings())))
                        .doOnSuccess(unused -> updateState(currentYdbConnectionState.withRollbackTransaction())))
                .then();
    }

    @Override
    public Mono<Void> updateState(Function<YdbConnectionState, YdbConnectionState> function) {
        return Mono.fromRunnable(() -> this.ydbConnectionState = function.apply(this.getCurrentState()));
    }

    @Override
    public YdbConnectionState getCurrentState() {
        return ydbConnectionState;
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return Mono.defer(() -> {
            final YdbConnectionState currentYdbConnectionState = ydbConnectionState;

            if (currentYdbConnectionState.getYdbTxSettings().isAutoCommit() == autoCommit) {
                return Mono.empty();
            }

            if (autoCommit && currentYdbConnectionState.isInTransaction()) {
                return commitTransaction();
            }

            updateState(currentYdbConnectionState.withAutoCommit(autoCommit));

            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            final YdbConnectionState currentYdbConnectionState = ydbConnectionState;

            if (currentYdbConnectionState.isClosed()) {
                return Mono.empty();
            }

            if (currentYdbConnectionState.isInTransaction()) {
                return commitTransaction()
                        .doOnSuccess(unused -> updateState(currentYdbConnectionState.close()));
            }

            updateState(currentYdbConnectionState.close());

            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        return Mono.fromRunnable(() -> this.statementTimeout = timeout);
    }

    private void updateState(YdbConnectionState ydbConnectionState) {
        this.ydbConnectionState = ydbConnectionState;
    }

    private <T extends RequestSettings<?>> T withStatementTimeout(T settings) {
        if (!statementTimeout.isZero() && !statementTimeout.isNegative()) {
            settings.setOperationTimeout(statementTimeout);
            settings.setTimeout(statementTimeout.plusSeconds(1));
        }

        return settings;
    }
}
