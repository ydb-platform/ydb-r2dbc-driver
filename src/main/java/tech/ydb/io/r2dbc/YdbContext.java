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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.r2dbc.spi.ConnectionFactoryOptions;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.Result;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.BalancingSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.io.r2dbc.options.ClientOptions;
import tech.ydb.io.r2dbc.options.ConnectionOptions;
import tech.ydb.io.r2dbc.util.YdbLookup;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.query.YdbSqlParser;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.table.Session;
import tech.ydb.table.impl.PooledTableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;

/**
 * @author Egor Kuleshov
 */
public class YdbContext {
    private static final int SESSION_POOL_DEFAULT_MIN_SIZE = 0;
    private static final int SESSION_POOL_DEFAULT_MAX_SIZE = 50;


    private final PooledTableClient tableClient;
    private final OperationsConfig operationsConfig;
    private final YdbTxSettings defaultYdbTxSettings;
    private final Cache<String, YdbQuery> queriesCache;

    public YdbContext(OptionExtractor optionExtractor) {
        this(buildClient(buildGrpcTransport(optionExtractor), optionExtractor), new OperationsConfig(optionExtractor));
    }


    public YdbContext(PooledTableClient tableClient, OperationsConfig operationsConfig) {
        this.tableClient = tableClient;
        this.operationsConfig = operationsConfig;
        this.defaultYdbTxSettings = YdbTxSettings.defaultSettings();
        if (operationsConfig.getStatementCacheSize() > 0) {
            this.queriesCache = CacheBuilder.newBuilder()
                    .maximumSize(operationsConfig.getStatementCacheSize())
                    .build();
        } else {
            this.queriesCache = null;
        }
    }

    public static GrpcTransport buildGrpcTransport(OptionExtractor optionExtractor) {
        Optional<String> schema = optionExtractor.extract(ConnectionFactoryOptions.PROTOCOL);

        GrpcTransportBuilder builder = GrpcTransport.forHost(
                (schema.map(s -> s + "://").orElse(""))
                        + optionExtractor.extractRequired(ConnectionFactoryOptions.HOST),
                optionExtractor.extractRequired(ConnectionFactoryOptions.PORT),
                optionExtractor.extractRequired(ConnectionFactoryOptions.DATABASE)
        );

        Optional<String> userOpt = optionExtractor.extract(ConnectionFactoryOptions.USER);
        Optional<CharSequence> password = optionExtractor.extract(ConnectionFactoryOptions.PASSWORD);


        optionExtractor.extract(ConnectionFactoryOptions.CONNECT_TIMEOUT)
                .ifPresent(builder::withConnectTimeout);
        optionExtractor.extractThenConsume(ConnectionOptions.GRPC_COMPRESSION, builder::withGrpcCompression);

        optionExtractor.extractThenConsume(ConnectionOptions.BALANCING_POLICY,
                policy -> builder.withBalancingSettings(BalancingSettings.fromPolicy(policy)));

        optionExtractor.extractThenConsume(ConnectionOptions.SECURE_CONNECTION_CERTIFICATE,
                builder::withSecureConnection);
        optionExtractor.extractThenConsume(ConnectionOptions.SECURE_CONNECTION_CERTIFICATE_FILE,
                certFile -> builder.withSecureConnection(YdbLookup.byteFileReference(certFile)));

        if (optionExtractor.extractOrDefault(ConnectionOptions.SECURE_CONNECTION, false)) {
            builder.withSecureConnection();
        }

        optionExtractor.extractThenConsume(ConnectionOptions.TOKEN,
                token -> builder.withAuthProvider(new TokenAuthProvider(YdbLookup.stringFileReference(token))));
        optionExtractor.extractThenConsume(ConnectionOptions.SERVICE_ACCOUNT_FILE,
                saFile -> builder.withAuthProvider(
                        CloudAuthHelper.getServiceAccountJsonAuthProvider(YdbLookup.stringFileReference(saFile))
                ));

        optionExtractor.extractThenConsume(ConnectionOptions.USE_METADATA, use -> {
            if (use) {
                builder.withAuthProvider(CloudAuthHelper.getMetadataAuthProvider());
            }
        });

        if (userOpt.isPresent() && !userOpt.get().isEmpty()) {
            builder.withAuthProvider(
                    new StaticCredentials(userOpt.get(), password.map(CharSequence::toString).orElse(null))
            );
        }

        return builder.build();
    }


    private static PooledTableClient buildClient(GrpcTransport grpcTransport, OptionExtractor optionExtractor) {
        PooledTableClient.Builder clientBuilder = PooledTableClient.newClient(GrpcTableRpc.useTransport(grpcTransport));
        optionExtractor.extractThenConsume(ClientOptions.KEEP_QUERY_TEXT, clientBuilder::keepQueryText);
        optionExtractor.extractThenConsume(ClientOptions.SESSION_KEEP_ALIVE_TIME, clientBuilder::sessionKeepAliveTime);
        optionExtractor.extractThenConsume(ClientOptions.SESSION_MAX_IDLE_TIME, clientBuilder::sessionMaxIdleTime);

        Optional<Integer> sessionPoolMinSize = optionExtractor.extract(ClientOptions.SESSION_POOL_MIN_SIZE);
        Optional<Integer> sessionPoolMaxSize = optionExtractor.extract(ClientOptions.SESSION_POOL_MAX_SIZE);

        int minSize = SESSION_POOL_DEFAULT_MIN_SIZE;
        int maxSize = SESSION_POOL_DEFAULT_MAX_SIZE;

        if (sessionPoolMinSize.isPresent()) {
            minSize = Math.max(0, sessionPoolMinSize.get());
            maxSize = Math.max(maxSize, minSize);
        }
        if (sessionPoolMaxSize.isPresent()) {
            maxSize = Math.max(minSize + 1, sessionPoolMaxSize.get());
        }

        clientBuilder.sessionPoolSize(minSize, maxSize);
        return clientBuilder.build();
    }

    public OperationsConfig getOperationsConfig() {
        return operationsConfig;
    }

    public CompletableFuture<Result<Session>> getSession() {
        return tableClient.createSession(operationsConfig.getSessionTimeout());
    }

    public Duration getStatementTimeout() {
        return operationsConfig.getStatementTimeout();
    }

    public Duration getDeadlineTimeout() {
        return operationsConfig.getDeadlineTimeout();
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
