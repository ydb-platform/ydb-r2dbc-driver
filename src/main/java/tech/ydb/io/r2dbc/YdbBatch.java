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

import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;
import tech.ydb.io.r2dbc.query.QueryType;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.statement.YdbDMLStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Kirill Kurdyukov
 */
public final class YdbBatch implements Batch {
    private final YdbConnection ydbConnection;
    private final YdbContext ydbContext;
    private final List<String> statements = new ArrayList<>();

    public YdbBatch(YdbConnection ydbConnection, YdbContext ydbContext) {
        this.ydbConnection = ydbConnection;
        this.ydbContext = ydbContext;
    }

    @Override
    public YdbBatch add(String sql) {
        statements.add(sql);

        return this;
    }

    @Override
    public Flux<YdbResult> execute() {
        YdbQuery query = ydbContext.findOrParseYdbQuery(String.join(";\n", this.statements));

        if (query.type() != QueryType.DML) {
            return Flux.error(new IllegalArgumentException("YDB support only DML batch queries"));
        }

        if (!query.getIndexArgNames().isEmpty()) {
            return Flux.error(new IllegalArgumentException("YDB does not support parametrized batch queries"));
        }

        return new YdbDMLStatement(query, ydbConnection).execute();
    }
}