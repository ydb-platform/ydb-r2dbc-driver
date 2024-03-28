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

import com.google.common.base.Preconditions;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.table.TableClient;

/**
 * @author Kirill Kurdyukov
 */
public final class YdbConnectionFactoryProvider implements ConnectionFactoryProvider {

    private static final String YDB_DRIVER = "ydb";

    private static final int SESSION_POOL_DEFAULT_MIN_SIZE = 10;
    private static final int SESSION_POOL_DEFAULT_MAX_SIZE = 100;

    private static final Option<Integer> SESSION_POOL_MIN_SIZE = Option.valueOf("sessionPoolMinSize");
    private static final Option<Integer> SESSION_POOL_MAX_SIZE = Option.valueOf("sessionPoolMaxSize");

    @Override
    public YdbConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
        OptionExtractor optionExtractor = new OptionExtractor(connectionFactoryOptions);

        GrpcTransportBuilder grpcTransportBuilder = GrpcTransport.forHost(
                optionExtractor.extractRequired(ConnectionFactoryOptions.HOST),
                optionExtractor.extractRequired(ConnectionFactoryOptions.PORT),
                optionExtractor.extractRequired(ConnectionFactoryOptions.DATABASE)
        );

        TableClient tableClient = TableClient.newClient(grpcTransportBuilder.build())
                .sessionPoolSize(
                        optionExtractor.extractOrDefault(SESSION_POOL_MIN_SIZE, SESSION_POOL_DEFAULT_MIN_SIZE),
                        optionExtractor.extractOrDefault(SESSION_POOL_MAX_SIZE, SESSION_POOL_DEFAULT_MAX_SIZE)
                )
                .build();

        return new YdbConnectionFactory(tableClient);
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        Preconditions.checkNotNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        Object driver = connectionFactoryOptions.getValue(ConnectionFactoryOptions.DRIVER);

        return driver != null && driver.equals(YDB_DRIVER);
    }

    @Override
    public String getDriver() {
        return YDB_DRIVER;
    }
}
