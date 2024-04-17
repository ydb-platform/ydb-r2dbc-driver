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

import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.YdbIsolationLevel;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.table.Session;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Egor Kuleshov
 */
public class CloseState implements YdbConnectionState {
    public static final String CLOSED_STATE_MESSAGE = "Connection closed";
    public static final CloseState INSTANCE = new CloseState();

    private CloseState() {
    }

    @Override
    public Mono<Session> getSession() {
        return Mono.error(new IllegalStateException(CLOSED_STATE_MESSAGE));
    }

    @Override
    public TxControl<?> txControl() {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public boolean isInTransaction() {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbTxSettings getYdbTxSettings() {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbConnectionState withDataQuery(String txId, Session session) {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbConnectionState withBeginTransaction(String id, Session session, YdbTxSettings ydbTxSettings) {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbConnectionState withCommitTransaction() {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbConnectionState withRollbackTransaction() {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbConnectionState withAutoCommit(boolean autoCommit) {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbConnectionState withIsolationLevel(YdbIsolationLevel isolationLevel) {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbConnectionState withReadOnly(boolean readOnly) {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public void withError(Session session) {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public YdbConnectionState close() {
        throw new IllegalStateException(CLOSED_STATE_MESSAGE);
    }

    @Override
    public boolean isClosed() {
        return true;
    }
}
