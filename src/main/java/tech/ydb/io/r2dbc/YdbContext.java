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
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import tech.ydb.core.Result;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.query.YdbSqlParser;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
/**
 * @author Egor Kuleshov
 */
public class YdbContext {
    private final TableClient tableClient;
    private final Duration createSessionTimeout;
    private final Duration defaultTimeout;
    private final YdbTxSettings defaultYdbTxSettings;
    private final Cache<String, YdbQuery> queriesCache;

    public YdbContext(TableClient tableClient,
                      Duration createSessionTimeout,
                      Duration defaultTimeout,
                      YdbTxSettings defaultYdbTxSettings,
                      int cacheSize) {
        this.tableClient = tableClient;
        this.createSessionTimeout = createSessionTimeout;
        this.defaultTimeout = defaultTimeout;
        this.defaultYdbTxSettings = defaultYdbTxSettings;
        if (cacheSize > 0) {
            queriesCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
        } else {
            queriesCache = null;
        }
    }

    @VisibleForTesting
    public YdbContext(TableClient tableClient) {
        this(tableClient, Duration.ofSeconds(5), Duration.ofSeconds(2), YdbTxSettings.defaultSettings(), 256);
    }

    public CompletableFuture<Result<Session>> getSession() {
        return tableClient.createSession(createSessionTimeout);
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

    public YdbQuery findOrParseYdbQuery(String sql) {
        if (queriesCache == null) {
            return YdbSqlParser.parse(sql);
        }

        YdbQuery cached = queriesCache.getIfPresent(sql);
        if (cached == null) {
            cached = YdbSqlParser.parse(sql);
            queriesCache.put(sql, cached);
        }

        return cached;
    }
}
