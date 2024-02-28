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

package tech.ydb.io.r2dbc.result;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import tech.ydb.table.query.DataQueryResult;

public class YdbDMLResult implements Result {
    private final DataQueryResult dataQueryResult;

    public YdbDMLResult(DataQueryResult dataQueryResult) {
        this.dataQueryResult = dataQueryResult;
    }

    @Override
    public Publisher<Long> getRowsUpdated() {
        return null;
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> biFunction) {
        return null;
    }

    @Override
    public Result filter(Predicate<Segment> predicate) {
        return null;
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> function) {
        return null;
    }
}
