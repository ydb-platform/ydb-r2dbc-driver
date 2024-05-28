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
import java.util.Objects;
import java.util.function.Function;

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.core.Result;
import tech.ydb.io.r2dbc.FluxDiscardOnCancel;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.util.ResultExtractor;
import tech.ydb.table.Session;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.BeginTxSettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.ExecuteSchemeQuerySettings;
import tech.ydb.table.settings.KeepAliveSessionSettings;

/**
 * Implementation of the connection state without an open transaction.
 * State for auto-commit mode or there is no open transaction.
 *
 * @author Egor Kuleshov
 */
public class OutsideTransactionState extends AbstractConnectionState implements YdbConnectionState {

    public OutsideTransactionState(YdbContext ydbContext, YdbTxSettings ydbTxSettings) {
        super(ydbContext, ydbTxSettings, ydbContext.getStatementTimeout());
    }

    public OutsideTransactionState(YdbContext ydbContext, YdbTxSettings ydbTxSettings, Duration statementTimeout) {
        super(ydbContext, ydbTxSettings, statementTimeout);
    }

    @Override
    public Mono<NextStateResult<Flux<YdbResult>>> executeDataQuery(String yql, Params params,
                                                                   List<OperationType> operationTypes) {
        return monoWithSession(session ->
                Mono.fromFuture(session.executeDataQuery(yql, ydbTxSettings.txControl(), params,
                                withStatementTimeout(new ExecuteDataQuerySettings())))
                        .map(dataQueryResult -> {
                            YdbConnectionState nextState;
                            if (dataQueryResult.getValue().getTxId() != null && !dataQueryResult.getValue().getTxId().isEmpty()) {
                                nextState = new InsideTransactionState(ydbContext,
                                        dataQueryResult.getValue().getTxId(),
                                        session,
                                        ydbTxSettings,
                                        statementTimeout);
                            } else {
                                nextState = this;
                                session.close();
                            }

                            return new NextStateResult<>(ResultExtractor.extract(
                                    dataQueryResult,
                                    operationTypes,
                                    ydbContext.getOperationsConfig().getFailOnTruncatedResult()),
                                    nextState);
                        })
        );
    }

    @Override
    public Flux<YdbResult> executeSchemeQuery(String yql) {
        return fluxWithSession(session -> Mono.fromFuture(session.executeSchemeQuery(yql,
                        withStatementTimeout(new ExecuteSchemeQuerySettings())))
                .flatMap(ResultExtractor::extract)
                .then(Mono.just(YdbResult.DDL_RESULT))
                .flux()
                .doOnComplete(session::close));
    }

    @Override
    public Mono<InsideTransactionState> beginTransaction(YdbTxSettings ydbTxSettings) {
        this.ydbTxSettings = ydbTxSettings.withAutoCommit(false);

        return monoWithSession(session -> Mono.fromFuture(session.beginTransaction(
                        ydbTxSettings.getMode(),
                        withDeadlineTimeout(new BeginTxSettings())))
                .map(Result::getValue)
                .map(transaction ->
                        new InsideTransactionState(ydbContext,
                                transaction.getId(),
                                session,
                                ydbTxSettings,
                                statementTimeout))
        );
    }

    @Override
    public Mono<OutsideTransactionState> commitTransaction() {
        return Mono.just(this);
    }

    @Override
    public Mono<OutsideTransactionState> rollbackTransaction() {
        return Mono.just(this);
    }

    @Override
    public Mono<YdbConnectionState> setAutoCommit(boolean autoCommit) {
        ydbTxSettings.setAutoCommit(autoCommit);

        return Mono.just(this);
    }

    @Override
    public Mono<Boolean> keepAlive(ValidationDepth depth) {
        return switch (depth) {
            case LOCAL -> Mono.just(true);
            case REMOTE -> monoWithSession(session ->
                    Mono.fromFuture(session.keepAlive(withStatementTimeout(new KeepAliveSessionSettings())))
                            .flatMap(stateResult -> ResultExtractor.extract(stateResult)
                                    .map(state -> Session.State.READY == state))
                            .doOnSuccess(unused -> session.close()));
        };
    }

    @Override
    public Mono<Void> setIsolationLevel(IsolationLevel isolationLevel) {
        return Mono.fromRunnable(() -> this.ydbTxSettings.setIsolationLevel(isolationLevel));
    }

    @Override
    public Mono<Void> setReadOnly(boolean readOnly) {
        return Mono.fromRunnable(() -> this.ydbTxSettings.setReadOnly(readOnly));
    }

    @Override
    public Mono<Void> close() {
        return Mono.empty();
    }

    /**
     * Apply function to session with correct processing and closing session.
     * Drain mono on cancel subscription.
     *
     * @param function applied to session
     * @param <T>      mono parameter
     * @return result function {@link Mono}
     */
    private <T> Mono<T> monoWithSession(Function<Session, Mono<T>> function) {
        return fluxWithSession(function.andThen(Mono::flux)).next();
    }

    /**
     * Apply function to session with correct processing and closing session.
     * Drain flux on cancel subscription.
     *
     * @param function applied to session
     * @param <T>      flux parameter
     * @return result function {@link Flux}
     */
    private <T> Flux<T> fluxWithSession(Function<Session, Flux<T>> function) {
        return Flux.defer(() -> Mono.fromFuture(ydbContext.getSession())
                .flatMap(sessionResult -> ResultExtractor.extract(sessionResult, "Error creating session"))
                .flatMapMany(session -> {
                    try {
                        return function.apply(session)
                                .doOnError(unused -> session.close());
                    } catch (Throwable t) {
                        session.close();

                        return Mono.error(t);
                    }
                }).as(FluxDiscardOnCancel::new));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OutsideTransactionState that = (OutsideTransactionState) o;
        return Objects.equals(ydbTxSettings, that.ydbTxSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ydbTxSettings);
    }

    @Override
    public String toString() {
        return "OutsideTransactionState{" +
                ", ydbTxSettings=" + ydbTxSettings +
                ", statementTimeout=" + statementTimeout +
                '}';
    }
}
