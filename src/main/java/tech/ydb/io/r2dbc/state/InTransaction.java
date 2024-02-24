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

import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.result.YdbDataResult;
import tech.ydb.io.r2dbc.result.YdbStatusResult;
import tech.ydb.table.Session;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;

/**
 * @author Kirill Kurdyukov
 */
final class InTransaction implements YdbConnectionState {
    public static final String SCHEME_QUERY_INSIDE_TRANSACTION = "Scheme query cannot be executed inside active "
            + "transaction. This behavior may be changed by property schemeQueryTxMode";

    private final Session session;
    private final String transactionId;

    InTransaction(Session session, String transactionId) {
        this.session = session;
        this.transactionId = transactionId;
    }

    @Override
    public Mono<YdbDataResult> executeDataQuery(String yql, Params params) {
        return Mono.fromFuture(session.executeDataQuery(yql, TxControl.id(transactionId), params))
                .map(dataQueryResultResult -> new YdbDataResult(dataQueryResultResult.getValue()));
    }

    @Override
    public Mono<YdbStatusResult> executeSchemaQuery(String yql, Params params) {
        return Mono.error(new IllegalStateException(SCHEME_QUERY_INSIDE_TRANSACTION));
    }
}
