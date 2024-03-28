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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.table.result.ResultSetReader;

public class YdbDMLResult implements Result {
    private final Flux<RowSegment> segments;

    public YdbDMLResult(ResultSetReader resultSetReader) {
        List<RowSegment> rowSegments = new ArrayList<>(resultSetReader.getRowCount());

        for (int index = 0; index < resultSetReader.getRowCount(); index++) {
            rowSegments.add(new RowSegment(new YdbRow(resultSetReader, index)));
        }

        segments = Flux.fromIterable(rowSegments);
    }

    public YdbDMLResult(Flux<RowSegment> segments) {
        this.segments = segments;
    }

    @Override
    public Publisher<Long> getRowsUpdated() {
        return Mono.just(-1L);
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> biFunction) {
        return segments.map(rowSegment -> biFunction.apply(rowSegment.row(), rowSegment.row.getMetadata()));
    }

    @Override
    public YdbDMLResult filter(Predicate<Segment> predicate) {
        return new YdbDMLResult(segments.filter(predicate));
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> function) {
        return segments.flatMap(function);
    }

    private static class RowSegment implements Result.RowSegment {

        final YdbRow row;

        RowSegment(YdbRow row) {
            this.row = row;
        }

        @Override
        public Row row() {
            return row;
        }
    }
}
