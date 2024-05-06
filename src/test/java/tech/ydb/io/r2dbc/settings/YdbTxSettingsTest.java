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

import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.ydb.io.r2dbc.YdbTransactionDefinition;
import tech.ydb.table.transaction.Transaction;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Egor Kuleshov
 */
public class YdbTxSettingsTest {
    @Test
    public void createTest() {
        YdbTxSettings ydbTxSettings = new YdbTxSettings(YdbIsolationLevel.SERIALIZABLE, false, true);

        Assertions.assertEquals(Transaction.Mode.SERIALIZABLE_READ_WRITE, ydbTxSettings.getMode());
        Assertions.assertEquals(TxControl.serializableRw(), ydbTxSettings.txControl());
    }

    @Test
    public void createFromTransactionDefinitionTest() {
        YdbTransactionDefinition ydbTransactionDefinition = new YdbTransactionDefinition()
                .isolationLevel(YdbIsolationLevel.ONLINE_INCONSISTENT_READ_ONLY)
                .readOnly(true);

        YdbTxSettings ydbTxSettings = new YdbTxSettings(ydbTransactionDefinition);

        Assertions.assertEquals(YdbIsolationLevel.ONLINE_INCONSISTENT_READ_ONLY, ydbTxSettings.getIsolationLevel());
        Assertions.assertTrue(ydbTxSettings.isReadOnly());
        Assertions.assertFalse(ydbTxSettings.isAutoCommit());
    }

    @Test
    public void createFromTransactionDefinitionWithoutReadOnlyTest() {
        YdbTransactionDefinition ydbTransactionDefinition = new YdbTransactionDefinition()
                .isolationLevel(YdbIsolationLevel.SERIALIZABLE);

        YdbTxSettings ydbTxSettings = new YdbTxSettings(ydbTransactionDefinition);

        Assertions.assertEquals(YdbIsolationLevel.SERIALIZABLE, ydbTxSettings.getIsolationLevel());
        Assertions.assertFalse(ydbTxSettings.isReadOnly());
        Assertions.assertFalse(ydbTxSettings.isAutoCommit());
    }

    @Test
    public void createFromTransactionDefinitionWithoutReadOnlyTwoTest() {
        YdbTransactionDefinition ydbTransactionDefinition = new YdbTransactionDefinition()
                .isolationLevel(YdbIsolationLevel.ONLINE_INCONSISTENT_READ_ONLY);

        YdbTxSettings ydbTxSettings = new YdbTxSettings(ydbTransactionDefinition);

        Assertions.assertEquals(YdbIsolationLevel.ONLINE_INCONSISTENT_READ_ONLY, ydbTxSettings.getIsolationLevel());
        Assertions.assertTrue(ydbTxSettings.isReadOnly());
        Assertions.assertFalse(ydbTxSettings.isAutoCommit());
    }

    @Test
    public void createFromTransactionDefinitionWithoutIsolationLevelTest() {
        YdbTransactionDefinition ydbTransactionDefinition = new YdbTransactionDefinition()
                .readOnly(true);

        Assertions.assertThrows(IllegalArgumentException.class, () -> new YdbTxSettings(ydbTransactionDefinition));
    }

    @Test
    public void setIsolationLevelTest() {
        YdbTxSettings ydbTxSettings = YdbTxSettings.defaultSettings();

        ydbTxSettings.setIsolationLevel(YdbIsolationLevel.SNAPSHOT_READ_ONLY);

        Assertions.assertEquals(YdbIsolationLevel.SNAPSHOT_READ_ONLY, ydbTxSettings.getIsolationLevel());
    }

    @Test
    public void setCustomIsolationLevelTest() {
        YdbTxSettings ydbTxSettings = YdbTxSettings.defaultSettings();

        ydbTxSettings.setIsolationLevel(IsolationLevel.valueOf("snapshotReadOnly"));

        Assertions.assertEquals(YdbIsolationLevel.SNAPSHOT_READ_ONLY, ydbTxSettings.getIsolationLevel());
    }

    @Test
    public void setCustomIsolationLevelErrorTest() {
        YdbTxSettings ydbTxSettings = YdbTxSettings.defaultSettings();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ydbTxSettings.setIsolationLevel(IsolationLevel.valueOf("notExistLevel")));
    }

    @ParameterizedTest
    @EnumSource(value = YdbIsolationLevelEnum.class, mode = EnumSource.Mode.EXCLUDE, names = {"SERIALIZABLE"})
    public void setReadOnlyIsolationLevelTest(YdbIsolationLevelEnum ydbIsolationLevel) {
        YdbTxSettings ydbTxSettings = new YdbTxSettings(YdbIsolationLevel.SERIALIZABLE, false, true);
        ydbTxSettings.setIsolationLevel(ydbIsolationLevel.isolationLevel());

        Assertions.assertTrue(ydbTxSettings.isReadOnly());
    }

    @Test
    public void setReadOnlyFalseTest() {
        YdbTxSettings ydbTxSettings = new YdbTxSettings(YdbIsolationLevel.SNAPSHOT_READ_ONLY, true, true);

        Assertions.assertThrows(IllegalArgumentException.class, () -> ydbTxSettings.setReadOnly(false));
    }

    @ParameterizedTest
    @EnumSource(YdbIsolationLevelEnum.class)
    public void txControlTest(YdbIsolationLevelEnum ydbIsolationLevel) {
        YdbTxSettings ydbTxSettings = YdbTxSettings.defaultSettings();
        ydbTxSettings.setIsolationLevel(ydbIsolationLevel.isolationLevel());

        ydbTxSettings.txControl();
    }
}
