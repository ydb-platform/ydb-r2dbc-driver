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

package tech.ydb.io.r2dbc;

import java.util.HashMap;
import java.util.Map;

import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;

/**
 * @author Egor Kuleshov
 */
public class YdbTransactionDefinition implements TransactionDefinition {
    public static final Option<YdbIsolationLevel> YDB_ISOLATION_LEVEL = Option.valueOf("ydbIsolationLevel");

    private final Map<Option<?>, Object> options;

    YdbTransactionDefinition(Map<Option<?>, Object> options) {
        this.options = options;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(Option<T> option) {
        return (T) this.options.get(option);
    }

    public YdbTransactionDefinition with(Option<?> option, Object value) {

        Map<Option<?>, Object> options = new HashMap<>(this.options);
        options.put(option, value);

        return new YdbTransactionDefinition(options);
    }

    public YdbTransactionDefinition isolationLevel(YdbIsolationLevel isolationLevel) {
        return with(YdbTransactionDefinition.YDB_ISOLATION_LEVEL, isolationLevel);
    }

    public YdbTransactionDefinition readOnly(boolean readOnly) {
        return with(YdbTransactionDefinition.READ_ONLY, readOnly);
    }
}
