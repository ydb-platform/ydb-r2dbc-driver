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

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import tech.ydb.io.r2dbc.parameter.YdbParameterResolver;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;

/**
 * @author Egor Kuleshov
 */
public final class YdbRow implements Row {
    private final ResultSetReader resultSetReader;
    private final int rowIndex;

    public YdbRow(ResultSetReader resultSetReader, int rowIndex) {
        this.resultSetReader = resultSetReader;
        this.rowIndex = rowIndex;
    }

    @Override
    public RowMetadata getMetadata() {
        return new YdbRowMetadata(resultSetReader);
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        resultSetReader.setRowIndex(rowIndex);
        ValueReader valueReader = resultSetReader.getColumn(index);
        if (valueReader.getValue().asOptional().isPresent()) {
            return YdbParameterResolver.resolveResult(valueReader, type);
        }

        return null;
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        return get(resultSetReader.getColumnIndex(name), type);
    }
}
