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

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author Egor Kuleshov
 */
public class BindingsImpl implements Bindings {
    @Override
    public Binding getCurrent() {
        return null;
    }

    @Override
    public void add(Binding binding) {}

    @Override
    public Iterator<Binding> iterator() {
        return null;
    }

    @Override
    public void forEach(Consumer<? super Binding> action) {
        Bindings.super.forEach(action);
    }

    @Override
    public Spliterator<Binding> spliterator() {
        return Bindings.super.spliterator();
    }
}
