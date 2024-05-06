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

package tech.ydb.io.r2dbc.settings;

import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import tech.ydb.io.r2dbc.YdbTransactionDefinition;
import tech.ydb.table.transaction.Transaction;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Egor Kuleshov
 */
public class YdbTxSettings {
    private volatile YdbIsolationLevelEnum isolationLevel;
    private volatile boolean readOnly;
    private volatile boolean autoCommit;

    public static YdbTxSettings defaultSettings() {
        return new YdbTxSettings(YdbIsolationLevelEnum.SERIALIZABLE, false, true);
    }

    @VisibleForTesting
    YdbTxSettings(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        this(YdbIsolationLevelEnum.valueOf(isolationLevel), readOnly, autoCommit);
    }

    private YdbTxSettings(YdbIsolationLevelEnum isolationLevel, boolean readOnly, boolean autoCommit) {
        Objects.requireNonNull(isolationLevel, "Expected isolation non null");
        validate(isolationLevel, readOnly);
        this.isolationLevel = isolationLevel;
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
    }

    public YdbTxSettings(TransactionDefinition transactionDefinition) {
        IsolationLevel ydbIsolationLevelOption =
                transactionDefinition.getAttribute(YdbTransactionDefinition.ISOLATION_LEVEL);
        if (ydbIsolationLevelOption == null) {
            throw new IllegalArgumentException("Expected ydbIsolation level in transaction definition, but not found");
        }

        this.isolationLevel = YdbIsolationLevelEnum.valueOf(ydbIsolationLevelOption);
        this.readOnly = isolationLevel.isReadOnly();
        Boolean readOnlyOption = transactionDefinition.getAttribute(TransactionDefinition.READ_ONLY);
        if (readOnlyOption != null) {
            this.readOnly = readOnlyOption;
        }

        validate(isolationLevel, readOnly);

        this.autoCommit = false;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel.isolationLevel();
    }

    public void setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = YdbIsolationLevelEnum.valueOf(isolationLevel);

        if (this.isolationLevel.isReadOnly()) {
            this.readOnly = true;
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        validate(isolationLevel, readOnly);

        this.readOnly = readOnly;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public YdbTxSettings withAutoCommit(boolean autoCommit) {
        return new YdbTxSettings(isolationLevel, readOnly, autoCommit);
    }

    public TxControl<?> txControl() {
        return txControl(isolationLevel, readOnly, autoCommit);
    }

    public Transaction.Mode getMode() {
        if (!readOnly) {
            validate(isolationLevel, readOnly);

            return Transaction.Mode.SERIALIZABLE_READ_WRITE;
        }

        return switch (isolationLevel) {
            case SERIALIZABLE, SNAPSHOT_READ_ONLY -> Transaction.Mode.SNAPSHOT_READ_ONLY;
            case ONLINE_CONSISTENT_READ_ONLY, ONLINE_INCONSISTENT_READ_ONLY -> Transaction.Mode.ONLINE_READ_ONLY;
            case STALE_READ_ONLY -> Transaction.Mode.STALE_READ_ONLY;
        };
    }

    private static void validate(YdbIsolationLevelEnum isolationLevel, boolean readOnly) {
        if (isolationLevel.isReadOnly() && !readOnly) {
            throw new IllegalArgumentException("Unsupported isolation level " + isolationLevel + " for read write " +
                    "mode");
        }
    }

    private static TxControl<?> txControl(YdbIsolationLevelEnum isolationLevel, boolean isReadOnly,
                                          boolean isAutoCommit) {
        validate(isolationLevel, isReadOnly);
        if (!isReadOnly) {
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

    @Override
    public String toString() {
        return "YdbTxSettings{" +
                "isolationLevel=" + isolationLevel +
                ", readOnly=" + readOnly +
                ", autoCommit=" + autoCommit +
                '}';
    }
}
