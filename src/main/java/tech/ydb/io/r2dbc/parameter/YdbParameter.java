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
import io.r2dbc.spi.Type;
import tech.ydb.io.r2dbc.type.YdbType;

/**
 * @author Egor Kuleshov
 */
public class YdbParameter implements Parameter {
    private final YdbType ydbType;
    private final Object value;

    public YdbParameter(YdbType ydbType, Object value) {
        this.ydbType = ydbType;
        this.value = value;
    }

    public YdbParameter(YdbType ydbType) {
        this.ydbType = ydbType;
        this.value = null;
    }

    @Override
    public Type getType() {
        return ydbType;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
