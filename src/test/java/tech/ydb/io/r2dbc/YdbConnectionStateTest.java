package tech.ydb.io.r2dbc;/*
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

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.statement.YdbStatement;

/**
 * @author Egor Kuleshov
 */
public class YdbConnectionStateTest {

    @Test
    public void connectionTest() {
        getConnection().block();
    }

    @Test
    public void connectionCreateAndDropTableTest() {
        Mono<YdbConnection> connection = getConnection();
        connection.map(ydbConnection -> ydbConnection
                        .createStatement("create table table_sample(id Int32, value Text, primary key (id))"))
                .flux()
                .flatMap(YdbStatement::execute)
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();

        connection.map(ydbConnection -> ydbConnection
                        .createStatement("drop table table_sample"))
                .flux()
                .flatMap(YdbStatement::execute)
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
    }

    private static Mono<YdbConnection> getConnection() {
        YdbConnectionFactoryProvider ydbConnectionFactoryProvider = new YdbConnectionFactoryProvider();
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "YDB")
                .option(ConnectionFactoryOptions.PROTOCOL, "grpc")
                .option(ConnectionFactoryOptions.HOST, "localhost")
                .option(ConnectionFactoryOptions.PORT, 2136)
                .option(ConnectionFactoryOptions.DATABASE, "local")
                .build();

        return ydbConnectionFactoryProvider.create(options).create();
    }
}
