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

/**
 * @author Egor Kuleshov
 */
public class YdbResult implements Result {
    public static final YdbResult UPDATE_RESULT = new YdbResult(Flux.empty(), 1L);
    public static final YdbResult DDL_RESULT = new YdbResult(Flux.empty(), 0L);
    private static final long DEFAULT_SELECT_ROWS_UPDATED = -1L;

    private final Flux<RowSegment> segments;
    private final long rowsUpdated;

    private YdbResult(Flux<RowSegment> segments, long rowsUpdated) {
        this.segments = segments;
        this.rowsUpdated = rowsUpdated;
    }

    public YdbResult(ResultSetReader resultSetReader) {
        List<RowSegment> rowSegments = new ArrayList<>(resultSetReader.getRowCount());

        for (int index = 0; index < resultSetReader.getRowCount(); index++) {
            rowSegments.add(new RowSegment(new YdbRow(resultSetReader, index)));
        }

        this.segments = Flux.fromIterable(rowSegments);
        this.rowsUpdated = DEFAULT_SELECT_ROWS_UPDATED;
    }

    /**
     * YDB do not support rows updated and return default value.
     *
     * @return default value rows updated by query type
     */
    @Override
    public Mono<Long> getRowsUpdated() {
        return Mono.just(rowsUpdated);
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> biFunction) {
        return segments.map(rowSegment -> biFunction.apply(rowSegment.row(), rowSegment.row.getMetadata()));
    }

    @Override
    public YdbResult filter(Predicate<Segment> predicate) {
        return new YdbResult(segments.filter(predicate), rowsUpdated);
    }

    @Override
    public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> function) {
        return segments.flatMap(function);
    }

    private static class RowSegment implements Result.RowSegment {

        private final YdbRow row;

        RowSegment(YdbRow row) {
            this.row = row;
        }

        @Override
        public Row row() {
            return row;
        }
    }
}