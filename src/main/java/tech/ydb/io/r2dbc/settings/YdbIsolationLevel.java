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
public class YdbIsolationLevel {
    public static final IsolationLevel SNAPSHOT_READ_ONLY = YdbIsolationLevelEnum.SNAPSHOT_READ_ONLY.isolationLevel();
    public static final IsolationLevel STALE_READ_ONLY = YdbIsolationLevelEnum.STALE_READ_ONLY.isolationLevel();
    public static final IsolationLevel ONLINE_INCONSISTENT_READ_ONLY = YdbIsolationLevelEnum.ONLINE_INCONSISTENT_READ_ONLY.isolationLevel();
    public static final IsolationLevel ONLINE_CONSISTENT_READ_ONLY = YdbIsolationLevelEnum.ONLINE_CONSISTENT_READ_ONLY.isolationLevel();
    public static final IsolationLevel SERIALIZABLE = YdbIsolationLevelEnum.SERIALIZABLE.isolationLevel();

    private YdbIsolationLevel() {
    }
}
