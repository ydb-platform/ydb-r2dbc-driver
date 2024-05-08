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

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import tech.ydb.io.r2dbc.parameter.YdbParameterResolver;
import tech.ydb.io.r2dbc.type.YdbType;
import tech.ydb.table.values.OptionalType;

/**
 * @author Kirill Kurdyukov
 */
public class YdbColumnMetadata implements ColumnMetadata {
    private final tech.ydb.table.values.Type type;
    private final String name;

    public YdbColumnMetadata(tech.ydb.table.values.Type type, String name) {
        this.type = type;
        this.name = name;
    }

    @Override
    public Class<?> getJavaType() {
        return YdbParameterResolver.resolveResultType(type).getJavaType();
    }

    @Override
    public YdbType getType() {
        return YdbParameterResolver.resolveResultType(type);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public tech.ydb.table.values.Type getNativeTypeMetadata() {
        return type;
    }

    @Override
    public Nullability getNullability() {
        return type instanceof OptionalType ? Nullability.NULLABLE : Nullability.NON_NULL;
    }
}
