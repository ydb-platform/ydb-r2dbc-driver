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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;

/**
 * @author Egor Kuleshov
 */
public interface Binding extends Params {
    void setParameter(int index, @Nullable Object obj, @Nonnull Type type);
    void setParameter(String name, @Nullable Object obj, @Nonnull Type type);

    void validate();
}
