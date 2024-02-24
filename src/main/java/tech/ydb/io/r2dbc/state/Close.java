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
import tech.ydb.table.query.Params;

/**
 * @author Kirill Kurdyukov
 */
final class Close implements YdbConnectionState {
    static final Close INSTANCE = new Close();

    @Override
    public Mono<YdbDataResult> executeDataQuery(String yql, Params params) {
        return Mono.error(new IllegalStateException("Connection is closed"));
    }

    @Override
    public Mono<YdbStatusResult> executeSchemaQuery(String yql, Params params) {
        return Mono.error(new IllegalStateException("Connection is closed"));
    }
}
