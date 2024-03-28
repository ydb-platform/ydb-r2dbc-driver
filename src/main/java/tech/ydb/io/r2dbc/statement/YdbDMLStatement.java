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

package tech.ydb.io.r2dbc.statement;

import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.YdbConnectionState;

/**
 * @author Egor Kuleshov
 */
public class YdbDMLStatement extends YdbStatement {
    public YdbDMLStatement(YdbQuery query, YdbConnectionState ydbConnectionState) {
        super(query, ydbConnectionState);
    }

    @Override
    public Flux<? extends Result> execute() {
        bindings.getCurrent().validate();

        return Flux.fromIterable(bindings)
                .flatMap(binding -> connectionState.executeDataQuery(query.getYqlQuery(bindings.getCurrent()),
                        bindings.getCurrent().toParams(), query.getOperationTypes()));
    }
}
