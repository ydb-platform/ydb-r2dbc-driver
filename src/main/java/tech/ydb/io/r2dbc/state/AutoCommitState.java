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
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.YdbIsolationLevel;
import tech.ydb.io.r2dbc.util.ResultExtractor;
import tech.ydb.table.Session;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Egor Kuleshov
 */
public class AutoCommitState implements YdbConnectionState {
    private final YdbTxSettings ydbTxSettings;
    private final YdbContext ydbContext;

    public AutoCommitState(YdbContext ydbContext, YdbTxSettings ydbTxSettings) {
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
    public YdbConnectionState withBeginTransaction(String id, Session session, YdbTxSettings ydbTxSettings) {
        return new InsideTransactionState(ydbContext, id, session, new YdbTxSettings(
                ydbTxSettings.getIsolationLevel(),
                ydbTxSettings.isReadOnly(),
                false));
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
        if (autoCommit) {
            return this;
        }

        return new OutsideTransactionState(ydbContext, ydbTxSettings);
    }

    @Override
    public YdbConnectionState withIsolationLevel(YdbIsolationLevel isolationLevel) {
        this.ydbTxSettings.setIsolationLevel(isolationLevel);

        return this;
    }

    @Override
    public YdbConnectionState close() {
        return new CloseState();
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
