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

package tech.ydb.io.r2dbc.state;

import java.time.Duration;

import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.result.YdbDataResult;
import tech.ydb.io.r2dbc.result.YdbStatusResult;
import tech.ydb.io.r2dbc.util.ResultExtractor;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Kirill Kurdyukov
 */
final class OutTransaction implements YdbConnectionState {

    private final TableClient tableClient;
    private final TxControl<?> txControl;
    private final Duration connectionTimeout;

    OutTransaction(TableClient tableClient, TxControl<?> txControl, Duration connectionTimeout) {
        this.tableClient = tableClient;
        this.txControl = txControl;
        this.connectionTimeout = connectionTimeout;
    }

    @Override
    public Mono<YdbDataResult> executeDataQuery(String yql, Params params) {
        return Mono.fromFuture(tableClient.createSession(connectionTimeout))
                .map(sessionResult -> ResultExtractor.extract(sessionResult, "Error creating session"))
                .flatMap(session -> Mono.fromFuture(session.executeDataQuery(yql, txControl, params)))
                .map(dataQueryResultResult -> new YdbDataResult(dataQueryResultResult.getValue()));
    }

    @Override
    public Mono<YdbStatusResult> executeSchemaQuery(String yql, Params params) {
        return Mono.fromFuture(tableClient.createSession(connectionTimeout))
                .map(sessionResult -> ResultExtractor.extract(sessionResult, "Error creating session"))
                .flatMap(session -> Mono.fromFuture(session.executeSchemeQuery(yql))).map(YdbStatusResult::new);
    }
}
