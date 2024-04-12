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

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

/**
 * Drains the source if the subscriber cancels the subscription
 *
 * @author Egor Kuleshov
 */
public class DiscardOnCancelSubscriber<T> extends AtomicBoolean implements CoreSubscriber<T>, Subscription {
    final CoreSubscriber<T> actual;
    final Context ctx;
    Subscription s;

    DiscardOnCancelSubscriber(CoreSubscriber<T> actual) {
        this.actual = actual;
        this.ctx = actual.currentContext();
    }

    @Override
    public Context currentContext() {
        return this.ctx;
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (reactor.core.publisher.Operators.validate(this.s, s)) {
            this.s = s;
            this.actual.onSubscribe(this);
        }
    }

    @Override
    public void onNext(T t) {
        if (this.get()) {
            reactor.core.publisher.Operators.onDiscard(t, this.ctx);
            return;
        }

        this.actual.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
        if (!this.get()) {
            this.actual.onError(t);
        }
    }

    @Override
    public void onComplete() {
        if (!this.get()) {
            this.actual.onComplete();
        }
    }

    @Override
    public void request(long n) {
        this.s.request(n);
    }

    @Override
    public void cancel() {
        if (compareAndSet(false, true)) {
            this.s.request(Long.MAX_VALUE);
        }
    }

}
