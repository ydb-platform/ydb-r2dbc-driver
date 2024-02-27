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

import java.sql.SQLException;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
    public Statement bind(int i, Object o) {
        return null;
    }

    @Override
    public Statement bind(String s, Object o) {
        return null;
    }

    @Override
    public Statement bindNull(int i, Class<?> aClass) {
        return null;
    }

    @Override
    public Statement bindNull(String s, Class<?> aClass) {
        return null;
    }

    @Override
    public Publisher<? extends Result> execute() {
        try {
            return Flux.fromIterable(bindings)
                    .flatMap(binding -> {
                        try {
                            return connectionState.executeDataQuery(query.getYqlQuery(bindings.getCurrent()),
                                            bindings.getCurrent());
                        } catch (SQLException e) {
                            return Mono.error(e);
                        }
                    });
        } catch (Exception e) {
            return Mono.error(e);
        }
    }
}
