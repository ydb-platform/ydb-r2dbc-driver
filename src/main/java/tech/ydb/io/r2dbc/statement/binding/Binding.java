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

package tech.ydb.io.r2dbc.statement.binding;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ydb.io.r2dbc.parameter.YdbParameterResolver;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Value;

/**
 * @author Egor Kuleshov
 */
public class Binding {
    private final Map<String, Value<?>> nameToValue = new HashMap<>();
    private final Set<String> unbounded;
    private final List<String> indexedNames;

    public Binding(List<String> indexedNames) {
        this.unbounded = new HashSet<>(indexedNames);
        this.indexedNames = indexedNames;
    }

    public void bind(int index, @Nullable Object obj) {
        bind(getNameByIndex(index), obj);
    }

    public void bind(String name, @Nullable Object obj) {
        put(name, YdbParameterResolver.resolve(obj));
    }

    public void bindNull(int index, @Nonnull Class<?> clazz) {
        bindNull(getNameByIndex(index), clazz);
    }

    public void bindNull(String name, @Nonnull Class<?> clazz) {
        put(name, YdbParameterResolver.resolveEmptyValue(clazz));
    }

    public void validate() {
        if (!unbounded.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Parameters %s not bounded", String.join(", ", unbounded)));
        }
    }

    public Map<String, Value<?>> values() {
        return nameToValue;
    }

    private void put(String name, Value<?> value) {
        if (!nameToValue.containsKey(name) && !unbounded.contains(name)) {
            throw new IllegalArgumentException(String.format("Parameter %s not existed", name));
        }

        nameToValue.put(name, value);
        unbounded.remove(name);
    }

    private String getNameByIndex(int index) {
        if (0 > index || index >= indexedNames.size()) {
            throw new IllegalArgumentException(String.format("Expected index between 0 and %s, but found %s",
                    indexedNames.size() - 1, index));
        }

        return indexedNames.get(index);
    }

    public Params toParams() {
        return Params.copyOf(nameToValue);
    }

    public static Binding empty() {
        return new Binding(List.of());
    }
}
