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

import reactor.core.publisher.Flux;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.YdbConnection;

/**
 * @author Egor Kuleshov
 */
public class YdbDMLStatement extends YdbStatement {
    public YdbDMLStatement(YdbQuery query, YdbConnection queryExecutor) {
        super(query, queryExecutor);
    }

    @Override
    public Flux<YdbResult> execute() {
        bindings.getCurrent().validate();

        String yql = query.getYqlQuery(bindings.getCurrent());
        return Flux.fromIterable(bindings)
                .concatMap(binding -> connection.executeDataQuery(
                                yql,
                                binding.toParams(),
                                query.getOperationTypes()
                        )
                );
    }
}
