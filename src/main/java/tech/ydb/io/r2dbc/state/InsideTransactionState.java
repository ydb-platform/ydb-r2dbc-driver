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
import tech.ydb.table.Session;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Egor Kuleshov
 */
public final class InsideTransactionState implements YdbConnectionState {
    private final String id;
    private final Session session;
    private final YdbTxSettings ydbTxSettings;
    private final TxControl.TxId txControl;
    private final YdbContext context;

    public InsideTransactionState(YdbContext context, String id, Session session, YdbTxSettings ydbTxSettings) {
        this.context = context;
        this.id = id;
        this.session = session;
        this.ydbTxSettings = ydbTxSettings;
        this.txControl = TxControl.id(id).setCommitTx(false);
    }

    @Override
    public Mono<Session> getSession() {
        return Mono.just(session);
    }

    @Override
    public TxControl<?> txControl() {
        return txControl;
    }

    @Override
    public boolean isInTransaction() {
        return true;
    }

    @Override
    public YdbTxSettings getYdbTxSettings() {
        return ydbTxSettings;
    }

    @Override
    public YdbConnectionState withDataQuery(String txId, Session session) {
        if (id.equals(txId)) {
            return this;
        }

        return new OutsideTransactionState(context, ydbTxSettings);
    }

    @Override
    public YdbConnectionState withBeginTransaction(String id, Session session, YdbTxSettings ydbTxSettings) {
        return this;
    }

    @Override
    public YdbConnectionState withCommitTransaction() {
        session.close();
        return new OutsideTransactionState(context, ydbTxSettings);
    }

    @Override
    public YdbConnectionState withRollbackTransaction() {
        session.close();
        return new OutsideTransactionState(context, ydbTxSettings);
    }

    @Override
    public YdbConnectionState withAutoCommit(boolean autoCommit) {
        if (autoCommit) {
            return new OutsideTransactionState(context, ydbTxSettings.withAutoCommit(true));
        }

        return this;
    }

    @Override
    public YdbConnectionState withIsolationLevel(YdbIsolationLevel isolationLevel) {
        if (ydbTxSettings.getIsolationLevel().equals(isolationLevel)) {
            return this;
        }

        throw new IllegalStateException("Can not change isolation level in active transaction");
    }

    @Override
    public YdbConnectionState withReadOnly(boolean readOnly) {
        if (ydbTxSettings.isReadOnly() == readOnly) {
            return this;
        }

        throw new IllegalStateException("Can not change read only in active transaction");
    }

    @Override
    public void withError(Session session) {
    }

    @Override
    public YdbConnectionState close() {
        return CloseState.INSTANCE;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    public String getId() {
        return id;
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
}
