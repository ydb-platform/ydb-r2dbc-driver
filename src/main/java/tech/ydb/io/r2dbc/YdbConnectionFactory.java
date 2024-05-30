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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.state.OutsideTransactionState;

/**
 * @author Kirill Kurdyukov
 */
public final class YdbConnectionFactory implements ConnectionFactory {

    private final YdbContext ydbContext;

    public YdbConnectionFactory(YdbContext ydbContext) {
        this.ydbContext = ydbContext;
    }

    @Override
    public Mono<YdbConnection> create() {
        return Mono.just(
                new YdbConnection(
                        ydbContext,
                        new OutsideTransactionState(ydbContext,
                                ydbContext.getDefaultYdbTxSettings(),
                                ydbContext.getStatementTimeout())
                )
        );
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return YdbConnectionFactoryMetadata.INSTANCE;
    }
}
