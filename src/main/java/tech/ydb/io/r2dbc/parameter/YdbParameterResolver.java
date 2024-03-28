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

package tech.ydb.io.r2dbc.parameter;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcType;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Objects;

import javax.annotation.Nonnull;

import tech.ydb.io.r2dbc.type.YdbType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * @author Kirill Kurdyukov
 */
public class YdbParameterResolver {

    private static final HashMap<Class<?>, YdbType> CLASS_YDB_TYPE = new HashMap<>(32);
    private static final HashMap<Type, YdbType> TYPE_YDB_TYPE = new HashMap<>(32);

    static {
        CLASS_YDB_TYPE.put(String.class, YdbType.TEXT);
        CLASS_YDB_TYPE.put(long.class, YdbType.INT64);
        CLASS_YDB_TYPE.put(Long.class, YdbType.INT64);
        CLASS_YDB_TYPE.put(byte.class, YdbType.INT8);
        CLASS_YDB_TYPE.put(Byte.class, YdbType.INT8);
        CLASS_YDB_TYPE.put(short.class, YdbType.INT16);
        CLASS_YDB_TYPE.put(Short.class, YdbType.INT16);
        CLASS_YDB_TYPE.put(int.class, YdbType.INT32);
        CLASS_YDB_TYPE.put(Integer.class, YdbType.INT32);
        CLASS_YDB_TYPE.put(float.class, YdbType.FLOAT);
        CLASS_YDB_TYPE.put(Float.class, YdbType.FLOAT);
        CLASS_YDB_TYPE.put(double.class, YdbType.DOUBLE);
        CLASS_YDB_TYPE.put(Double.class, YdbType.DOUBLE);
        CLASS_YDB_TYPE.put(boolean.class, YdbType.BOOL);
        CLASS_YDB_TYPE.put(Boolean.class, YdbType.BOOL);
        CLASS_YDB_TYPE.put(byte[].class, YdbType.BYTES);
        CLASS_YDB_TYPE.put(Date.class, YdbType.TIMESTAMP);
        CLASS_YDB_TYPE.put(LocalDate.class, YdbType.DATE);
        CLASS_YDB_TYPE.put(LocalDateTime.class, YdbType.DATETIME);
        CLASS_YDB_TYPE.put(BigDecimal.class, YdbType.DECIMAL);
        CLASS_YDB_TYPE.put(Duration.class, YdbType.INTERVAL);

        for(YdbType ydbType : YdbType.values()) {
            TYPE_YDB_TYPE.put(ydbType.getYdbType(), ydbType);
        }
    }

    private YdbParameterResolver() {
    }

    public static Value<?> resolve(Object param) {
        if (param instanceof Value<?> value) {
            return value;
        } else if (param instanceof Parameter parameter) {
            return resolveParameter(parameter);
        }

        return resolveClass(param.getClass()).createValue(param);
    }

    public static <T> T resolveResult(Value<?> value) {
        return TYPE_YDB_TYPE.get(value.getType()).getObject(value);
    }

    public static Value<?> resolveEmptyValue(Class<?> clazz) {
        return resolveClass(clazz).getYdbType().makeOptional().emptyValue();
    }

    public static YdbType resolveClass(Class<?> clazz) {
        if (CLASS_YDB_TYPE.containsKey(clazz)) {
            return CLASS_YDB_TYPE.get(clazz);
        } else {
            throw new IllegalArgumentException("Could not resolve" + clazz.getName() + "class to YdbType");
        }
    }

    private static Value<?> resolveParameter(@Nonnull Parameter parameter) {
        if (parameter.getType() instanceof YdbType ydbType) {
            if (parameter.getValue() == null) {
                return ydbType.getYdbType().makeOptional().emptyValue();
            }
            return ydbType.createValue(Objects.requireNonNull(parameter.getValue()));
        } else if (parameter.getType() instanceof R2dbcType r2dbcType) {
            if (parameter.getValue() == null) {
                return YdbType.valueOf(r2dbcType).getYdbType().makeOptional().emptyValue();
            }

            return YdbType.valueOf(r2dbcType).createValue(parameter.getValue());
        } else {
            if (parameter.getValue() == null) {
                return resolveClass(parameter.getType().getJavaType()).getYdbType().makeOptional().emptyValue();
            }
            return resolveClass(parameter.getType().getJavaType()).createValue(Objects.requireNonNull(parameter.getValue()));
        }
    }
}
