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

/**
 * @author Egor Kuleshov
 */
enum YdbIsolationLevelEnum {
    SNAPSHOT_READ_ONLY(true, IsolationLevel.valueOf("snapshotReadOnly")),
    STALE_READ_ONLY(true, IsolationLevel.valueOf("staleReadOnly")),
    ONLINE_INCONSISTENT_READ_ONLY(true, IsolationLevel.valueOf("onlineInconsistentReadOnly")),
    ONLINE_CONSISTENT_READ_ONLY(true, IsolationLevel.valueOf("onlineConsistentReadOnly")),
    SERIALIZABLE(false, IsolationLevel.SERIALIZABLE);

    private final boolean readOnly;
    private final IsolationLevel isolationLevel;

    YdbIsolationLevelEnum(boolean readOnly, IsolationLevel isolationLevel) {
        this.readOnly = readOnly;
        this.isolationLevel = isolationLevel;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public static YdbIsolationLevelEnum valueOf(IsolationLevel isolationLevel) {
        for (YdbIsolationLevelEnum ydbIsolationLevel : YdbIsolationLevelEnum.values()) {
            if (ydbIsolationLevel.isolationLevel.equals(isolationLevel)) {
                return ydbIsolationLevel;
            }
        }

        throw new IllegalArgumentException("Unexpected isolation level, expected YdbIsolationLevel, but found: " + isolationLevel);
    }

    @Override
    public String toString() {
        return "YdbIsolationLevel{" +
                "sql='" + this.name() + '\'' +
                '}';
    }
}
