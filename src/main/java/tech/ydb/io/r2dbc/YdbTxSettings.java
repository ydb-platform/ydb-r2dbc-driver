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

import java.util.Objects;

import io.r2dbc.spi.TransactionDefinition;
import tech.ydb.table.transaction.Transaction;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Egor Kuleshov
 */
public class YdbTxSettings {
    public static final YdbTxSettings DEFAULT = new YdbTxSettings(YdbIsolationLevel.SERIALIZABLE, false, true);
    private volatile YdbIsolationLevel isolationLevel;
    private final boolean readOnly;
    private final boolean autoCommit;

    public YdbTxSettings(YdbIsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        this.isolationLevel = isolationLevel;
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
    }

    public YdbTxSettings(TransactionDefinition transactionDefinition) {
        this.isolationLevel = transactionDefinition.getAttribute(YdbTransactionDefinition.YDB_ISOLATION_LEVEL);
        this.readOnly =  Boolean.TRUE.equals(transactionDefinition.getAttribute(TransactionDefinition.READ_ONLY));
        this.autoCommit = false;
    }

    public YdbIsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(YdbIsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public YdbTxSettings withAutoCommit(boolean autoCommit) {
        return new YdbTxSettings(isolationLevel, readOnly, autoCommit);
    }

    public TxControl<?> txControl() {
        return txControl(isolationLevel, readOnly, autoCommit);
    }

    public Transaction.Mode getMode() {
        if (!readOnly) {
            if (!isolationLevel.equals(YdbIsolationLevel.SERIALIZABLE)) {
                throw new IllegalStateException("Unsupported isolation level " + isolationLevel + " for read write mode");
            }

            return Transaction.Mode.SERIALIZABLE_READ_WRITE;
        }

        return switch (isolationLevel) {
            case SERIALIZABLE, SNAPSHOT_READ_ONLY -> Transaction.Mode.SNAPSHOT_READ_ONLY;
            case ONLINE_CONSISTENT_READ_ONLY, ONLINE_INCONSISTENT_READ_ONLY -> Transaction.Mode.ONLINE_READ_ONLY;
            case STALE_READ_ONLY -> Transaction.Mode.STALE_READ_ONLY;
        };
    }

    private static TxControl<?> txControl(YdbIsolationLevel isolationLevel, boolean isReadOnly, boolean isAutoCommit) {
        if (!isReadOnly) {
            if (!isolationLevel.equals(YdbIsolationLevel.SERIALIZABLE)) {
                throw new IllegalStateException("Unsupported isolation level " + isolationLevel + " for read write mode");
            }

            return TxControl.serializableRw().setCommitTx(isAutoCommit);
        }

        return switch (isolationLevel) {
            case SERIALIZABLE -> TxControl.snapshotRo().setCommitTx(isAutoCommit);
            case ONLINE_CONSISTENT_READ_ONLY ->
                    TxControl.onlineRo().setAllowInconsistentReads(false).setCommitTx(isAutoCommit);
            case ONLINE_INCONSISTENT_READ_ONLY ->
                    TxControl.onlineRo().setAllowInconsistentReads(true).setCommitTx(isAutoCommit);
            case STALE_READ_ONLY -> TxControl.staleRo().setCommitTx(isAutoCommit);
            case SNAPSHOT_READ_ONLY -> TxControl.snapshotRo();
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        YdbTxSettings that = (YdbTxSettings) o;
        return readOnly == that.readOnly && autoCommit == that.autoCommit && isolationLevel == that.isolationLevel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isolationLevel, readOnly, autoCommit);
    }
}
