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

import java.util.Objects;

import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.YdbIsolationLevel;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.io.r2dbc.util.ResultExtractor;
import tech.ydb.table.Session;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Egor Kuleshov
 */
public class OutsideTransactionState implements YdbConnectionState {
    protected final YdbTxSettings ydbTxSettings;
    protected final YdbContext ydbContext;

    public OutsideTransactionState(YdbContext ydbContext, YdbTxSettings ydbTxSettings) {
        this.ydbContext = ydbContext;
        this.ydbTxSettings = ydbTxSettings;
    }

    @Override
    public Mono<Session> getSession() {
        return Mono.fromFuture(ydbContext.getTableClient().createSession(ydbContext.getCreateSessionTimeout()))
                .flatMap(sessionResult -> ResultExtractor.extract(sessionResult, "Error creating session"));
    }

    @Override
    public TxControl<?> txControl() {
        return ydbTxSettings.txControl();
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }

    public YdbTxSettings getYdbTxSettings() {
        return ydbTxSettings;
    }

    @Override
    public YdbConnectionState withDataQuery(String txId, Session session) {
        if (txId == null || txId.isEmpty()) {
            return this;
        }

        return new InsideTransactionState(ydbContext, txId, session, ydbTxSettings.withAutoCommit(false));
    }

    @Override
    public YdbConnectionState withBeginTransaction(String id, Session session, YdbTxSettings ydbTxSettings) {
        return new InsideTransactionState(ydbContext, id, session, ydbTxSettings);
    }

    @Override
    public YdbConnectionState withCommitTransaction() {
        return this;
    }

    @Override
    public YdbConnectionState withRollbackTransaction() {
        return this;
    }

    @Override
    public YdbConnectionState withAutoCommit(boolean autoCommit) {
        if (ydbTxSettings.isAutoCommit() == autoCommit) {
            return this;
        }

        return new OutsideTransactionState(ydbContext, ydbTxSettings.withAutoCommit(autoCommit));
    }

    @Override
    public YdbConnectionState withIsolationLevel(YdbIsolationLevel isolationLevel) {
        this.ydbTxSettings.setIsolationLevel(isolationLevel);

        return this;
    }

    @Override
    public YdbConnectionState close() {
        return CloseState.INSTANCE;
    }

    @Override
    public boolean isClosed() {
        return false;
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
}
