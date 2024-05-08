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

import java.util.List;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.Value;

/**
 * @author Egor Kuleshov
 */
public final class YdbRow implements Row {
    private final YdbRowMetadata ydbRowMetadata;
    private final List<Value<?>> values;

    public YdbRow(YdbRowMetadata ydbRowMetadata, List<Value<?>> values) {
        this.ydbRowMetadata = ydbRowMetadata;
        this.values = values;
    }

    @Override
    public RowMetadata getMetadata() {
        return ydbRowMetadata;
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        Value<?> value = values.get(index);
        if (value instanceof OptionalValue) {
            if (!value.asOptional().isPresent()) {
                return null;
            } else {
                value = value.asOptional().get();
            }
        }

        return type.cast(ydbRowMetadata.getColumnMetadata(index).getType().getObject(value));
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        return get(ydbRowMetadata.getColumnIndex(name), type);
    }
}
