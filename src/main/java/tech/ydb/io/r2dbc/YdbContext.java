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

import java.time.Duration;

import tech.ydb.io.r2dbc.state.YdbTxSettings;
import tech.ydb.table.TableClient;
/**
 * @author Egor Kuleshov
 */
public class YdbContext {
    private final TableClient tableClient;
    private final Duration createSessionTimeout;
    private final Duration defaultTimeout;
    private final YdbTxSettings defaultYdbTxSettings;

    public YdbContext(TableClient tableClient, Duration createSessionTimeout, Duration defaultTimeout,
                      YdbTxSettings defaultYdbTxSettings) {
        this.tableClient = tableClient;
        this.createSessionTimeout = createSessionTimeout;
        this.defaultTimeout = defaultTimeout;
        this.defaultYdbTxSettings = defaultYdbTxSettings;
    }


    public YdbContext(TableClient tableClient) {
        this(tableClient, Duration.ofSeconds(5), Duration.ofSeconds(2), YdbTxSettings.DEFAULT);
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public Duration getCreateSessionTimeout() {
        return createSessionTimeout;
    }

    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    public YdbTxSettings getDefaultYdbTxSettings() {
        return defaultYdbTxSettings;
    }
}
