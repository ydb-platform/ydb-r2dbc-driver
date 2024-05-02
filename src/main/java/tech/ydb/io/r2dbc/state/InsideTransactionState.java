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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.YdbIsolationLevel;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.util.ResultExtractor;
import tech.ydb.table.Session;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.RollbackTxSettings;
import tech.ydb.table.transaction.TxControl;

/**
 * Implementation of the connection state in an open transaction.
 *
 * @author Egor Kuleshov
 */
public final class InsideTransactionState extends AbstractConnectionState implements YdbConnectionState {
    private static final String SCHEME_QUERY_INSIDE_TRANSACTION = "Scheme query cannot be executed inside active "
            + "transaction. This behavior may be changed by property schemeQueryTxMode";

    private final String id;
    private final Session session;
    private final TxControl.TxId txControl;

    public InsideTransactionState(YdbContext ydbContext, String id, Session session, YdbTxSettings ydbTxSettings) {
        super(ydbContext, ydbTxSettings, ydbContext.getDefaultTimeout());
        this.id = id;
        this.session = session;
        this.ydbTxSettings = ydbTxSettings;
        this.txControl = TxControl.id(id).setCommitTx(false);
    }

    public InsideTransactionState(YdbContext ydbContext,
                                  String id,
                                  Session session,
                                  YdbTxSettings ydbTxSettings,
                                  Duration statementTimeout) {
        super(ydbContext, ydbTxSettings, statementTimeout);
        this.id = id;
        this.session = session;
        this.txControl = TxControl.id(id).setCommitTx(false);
    }

    @Override
    public Mono<NextStateResult<Flux<YdbResult>>> executeDataQuery(String yql,
                                                                   Params params,
                                                                   List<OperationType> operationTypes) {
        return Mono.fromFuture(session.executeDataQuery(yql, txControl, params,
                        withStatementTimeout(new ExecuteDataQuerySettings())))
                .map(dataQueryResult -> {
                    String txId = dataQueryResult.getValue().getTxId();
                    YdbConnectionState nextState = this;
                    if (txId != null && !txId.isEmpty() && !txId.equals(this.id)) {
                        nextState = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
                    }
                    if (dataQueryResult.getValue().getTxId() == null || dataQueryResult.getValue().getTxId().isEmpty()) {
                        nextState = new OutsideTransactionState(ydbContext, ydbTxSettings, statementTimeout);
                        session.close();
                    }

                    return new NextStateResult<>(ResultExtractor.extract(dataQueryResult, operationTypes), nextState);
                });
    }

    @Override
    public Flux<YdbResult> executeSchemaQuery(String yql) {
        return Flux.error(new IllegalStateException(SCHEME_QUERY_INSIDE_TRANSACTION));
    }

    @Override
    public Mono<InsideTransactionState> beginTransaction(YdbTxSettings ydbTxSettings) {
        return Mono.just(this);
    }

    @Override
    public Mono<OutsideTransactionState> commitTransaction() {
        return Mono.fromFuture(session.commitTransaction(
                        id,
                        withStatementTimeout(new CommitTxSettings())))
                .flatMap(ResultExtractor::extract)
                .doOnSuccess(unused -> session.close())
                .then(Mono.just(new OutsideTransactionState(ydbContext, ydbTxSettings, statementTimeout)));
    }

    @Override
    public Mono<OutsideTransactionState> rollbackTransaction() {
        return Mono.fromFuture(session.rollbackTransaction(
                        id,
                        withStatementTimeout(new RollbackTxSettings())))
                .flatMap(ResultExtractor::extract)
                .doOnSuccess(unused -> session.close())
                .then(Mono.just(new OutsideTransactionState(ydbContext, ydbTxSettings, statementTimeout)));
    }

    @Override
    public Mono<YdbConnectionState> setAutoCommit(boolean autoCommit) {
        if (autoCommit) {
            return commitTransaction()
                    .flatMap(state -> state.setAutoCommit(true));
        }

        return Mono.just(this);
    }

    @Override
    public Mono<Void> setIsolationLevel(YdbIsolationLevel isolationLevel) {
        if (ydbTxSettings.getIsolationLevel().equals(isolationLevel)) {
            return Mono.empty();
        }

        return Mono.error(new IllegalStateException("Can not change isolation level in active transaction"));
    }

    @Override
    public Mono<Void> setReadOnly(boolean readOnly) {
        if (ydbTxSettings.isReadOnly() == readOnly) {
            return Mono.empty();
        }

        return Mono.error(new IllegalStateException("Can not change read only in active transaction"));
    }

    public Mono<Void> close() {
        return commitTransaction()
                .then();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InsideTransactionState that = (InsideTransactionState) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "InsideTransactionState{" +
                "id='" + id + '\'' +
                ", session=" + session +
                ", txControl=" + txControl +
                ", ydbTxSettings=" + ydbTxSettings +
                '}';
    }
}
