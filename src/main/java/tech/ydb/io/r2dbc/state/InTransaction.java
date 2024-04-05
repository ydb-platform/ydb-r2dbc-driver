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

import java.util.ArrayList;
import java.util.List;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.util.ResultExtractor;
import tech.ydb.table.Session;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Kirill Kurdyukov
 */
public final class InTransaction implements YdbConnectionState {
    public static final String SCHEME_QUERY_INSIDE_TRANSACTION = "Scheme query cannot be executed inside active "
            + "transaction. This behavior may be changed by property schemeQueryTxMode";

    private final Session session;
    private final String transactionId;

    InTransaction(Session session, String transactionId) {
        this.session = session;
        this.transactionId = transactionId;
    }

    @Override
    public Flux<YdbResult> executeDataQuery(String yql, Params params, List<OperationType> operationTypes) {
        return Mono.fromFuture(session.executeDataQuery(yql, TxControl.id(transactionId), params))
                .flatMapMany(dataQueryResult -> {
                    Mono<DataQueryResult> dataQueryResultMono = ResultExtractor.extract(dataQueryResult);

                    return dataQueryResultMono.flatMapMany(result -> {
                        List<YdbResult> results = new ArrayList<>();
                        for (int opIndex = 0, resSetIndex = 0; opIndex < operationTypes.size(); opIndex++) {
                            results.add(switch (operationTypes.get(opIndex)) {
                                case SELECT -> {
                                    resSetIndex++;
                                    yield new YdbResult(result.getResultSet(resSetIndex));
                                }
                                case UPDATE -> YdbResult.UPDATE_RESULT;
                                case SCHEME ->
                                        throw new IllegalStateException("DDL operation not support in " +
                                                "executeDataQuery");
                            });
                        }

                        return Flux.fromIterable(results);
                    });
                });
    }

    @Override
    public Mono<YdbResult> executeSchemaQuery(String yql) {
        return Mono.error(new IllegalStateException(SCHEME_QUERY_INSIDE_TRANSACTION));
    }
}
