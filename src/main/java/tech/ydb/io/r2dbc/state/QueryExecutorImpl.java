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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.util.ResultExtractor;
import tech.ydb.table.query.DataQueryResult;
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
    public Flux<YdbResult> executeDataQuery(String yql, Params params, List<OperationType> operationTypes) {
        return fluxWithState(connectionState -> connectionState.getSession()
                .flatMapMany(session ->
                        Mono.fromFuture(session.executeDataQuery(yql, connectionState.txControl(), params,
                                        withStatementTimeout(new ExecuteDataQuerySettings())))
                                .flatMapMany(dataQueryResult -> {
                                    Mono<DataQueryResult> dataQueryResultMono =
                                            ResultExtractor.extract(dataQueryResult);

                                    return dataQueryResultMono.flatMapMany(result -> {
                                        List<YdbResult> results = new ArrayList<>();
                                        for (int opIndex = 0, resSetIndex = 0; opIndex < operationTypes.size(); opIndex++) {
                                            results.add(switch (operationTypes.get(opIndex)) {
                                                case SELECT -> new YdbResult(result.getResultSet(resSetIndex++));
                                                case UPDATE -> YdbResult.UPDATE_RESULT;
                                                case SCHEME -> throw new IllegalStateException("DDL operation not" +
                                                        " support in" +
                                                        " " +
                                                        "executeDataQuery");
                                            });
                                        }

                                        return Flux.fromIterable(results);
                                    });
                                })
                                .doFinally((signalType) -> session.close())
                )
        );
    }

    @Override
    public Flux<YdbResult> executeSchemaQuery(String yql) {
        return fluxWithState(connectionState -> {

            if (connectionState.isInTransaction()) {
                return Flux.error(new IllegalStateException(SCHEME_QUERY_INSIDE_TRANSACTION));
            }

            return connectionState.getSession()
                    .flatMap(session -> Mono.fromFuture(session.executeSchemeQuery(yql,
                            withStatementTimeout(new ExecuteSchemeQuerySettings()))))
                    .flatMap(status -> {
                        if (!status.isSuccess()) {
                            return Mono.error(new UnexpectedResultException("Schema query failed", status));
                        }

                        return Mono.just(YdbResult.DDL_RESULT);
                    }).flux();
        });
    }

    @Override
    public Mono<Void> beginTransaction() {
        return monoWithState(connectionState ->
                beginTransaction(connectionState, connectionState.getYdbTxSettings()));
    }

    @Override
    public Mono<Void> beginTransaction(YdbTxSettings ydbTxSettings) {
        return monoWithState(connectionState -> beginTransaction(connectionState, ydbTxSettings));
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
        return monoWithState(connectionState -> {

            if (!connectionState.isInTransaction()) {
                return Mono.empty();
            }
            final InsideTransactionState insideTransactionState = (InsideTransactionState) connectionState;

            return connectionState.getSession()
                    .flatMap(session -> Mono.fromFuture(session.commitTransaction(
                                    insideTransactionState.getId(),
                                    withStatementTimeout(new CommitTxSettings())))
                            .doOnSuccess(unused -> updateState(connectionState.withCommitTransaction())))
                    .then();
        });
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return monoWithState(connectionState -> {
            if (!connectionState.isInTransaction()) {
                return Mono.empty();
            }
            final InsideTransactionState insideTransactionState = (InsideTransactionState) connectionState;

            return connectionState.getSession()
                    .flatMap(session -> Mono.fromFuture(session.rollbackTransaction(
                                    insideTransactionState.getId(),
                                    withStatementTimeout(new RollbackTxSettings())))
                            .doOnSuccess(unused -> updateState(connectionState.withRollbackTransaction())))
                    .then();
        });
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
        return monoWithState(connectionState -> {

            if (connectionState.getYdbTxSettings().isAutoCommit() == autoCommit) {
                return Mono.empty();
            }

            if (autoCommit && connectionState.isInTransaction()) {
                return commitTransaction();
            }

            updateState(connectionState.withAutoCommit(autoCommit));

            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> close() {
        return monoWithState(connectionState -> {
            if (connectionState.isInTransaction()) {
                return commitTransaction()
                        .doOnSuccess(unused -> updateState(connectionState.close()));
            }

            updateState(connectionState.close());

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

    private <T> Mono<T> monoWithState(Function<YdbConnectionState, Mono<T>> function) {
        final YdbConnectionState currentYdbConnectionState = ydbConnectionState;
        if (currentYdbConnectionState.isClosed()) {
            return Mono.error(new IllegalStateException(CloseState.CLOSED_STATE_MESSAGE));
        }

        return function.apply(currentYdbConnectionState);
    }

    private <T> Flux<T> fluxWithState(Function<YdbConnectionState, Flux<T>> function) {
        final YdbConnectionState currentYdbConnectionState = ydbConnectionState;
        if (currentYdbConnectionState.isClosed()) {
            return Flux.error(new IllegalStateException(CloseState.CLOSED_STATE_MESSAGE));
        }

        return function.apply(currentYdbConnectionState);
    }

    private <T extends RequestSettings<?>> T withStatementTimeout(T settings) {
        if (!statementTimeout.isZero() && !statementTimeout.isNegative()) {
            settings.setOperationTimeout(statementTimeout);
            settings.setTimeout(statementTimeout.plusSeconds(1));
        }

        return settings;
    }
}
