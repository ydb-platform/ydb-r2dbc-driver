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

import java.util.List;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.table.query.Params;

/**
 * @author Kirill Kurdyukov
 */
public final class Close implements YdbConnectionState {
    static final Close INSTANCE = new Close();

    @Override
    public Flux<YdbResult> executeDataQuery(String yql, Params params, List<OperationType> expressionTypes) {
        return Flux.error(new IllegalStateException("Connection is closed"));
    }

    @Override
    public Mono<YdbResult> executeSchemaQuery(String yql) {
        return Mono.error(new IllegalStateException("Connection is closed"));
    }
}
