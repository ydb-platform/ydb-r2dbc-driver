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

import io.r2dbc.spi.RowMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Egor Kuleshov
 */
public final class YdbRowMetadata implements RowMetadata {
    private final List<YdbColumnMetadata> ydbColumnMetadatas;
    private final Map<String, Integer> nameToIndex;

    public YdbRowMetadata(List<YdbColumnMetadata> ydbColumnMetadatas) {
        this.ydbColumnMetadatas = ydbColumnMetadatas;
        this.nameToIndex = new HashMap<>(ydbColumnMetadatas.size());
        for (int index = 0; index < ydbColumnMetadatas.size(); index++) {
            nameToIndex.put(ydbColumnMetadatas.get(index).getName(), index);
        }
    }

    @Override
    public YdbColumnMetadata getColumnMetadata(int index) {
        Objects.checkIndex(index, ydbColumnMetadatas.size());
        return ydbColumnMetadatas.get(index);
    }

    @Override
    public YdbColumnMetadata getColumnMetadata(String name) {
        validateColumnName(name);

        return ydbColumnMetadatas.get(nameToIndex.get(name));
    }

    @Override
    public List<YdbColumnMetadata> getColumnMetadatas() {
        return ydbColumnMetadatas;
    }

    @Override
    public boolean contains(String columnName) {
        return nameToIndex.containsKey(columnName);
    }

    public int getColumnIndex(String name) {
        validateColumnName(name);

        return nameToIndex.get(name);
    }

    private void validateColumnName(String name) {
        if (!contains(name)) {
            throw new IllegalArgumentException(String.format("Column with name '%s' does not exist", name));
        }
    }
}
