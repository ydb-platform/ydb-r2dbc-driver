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

package tech.ydb.io.r2dbc.util;

import reactor.core.publisher.Mono;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.UnexpectedResultException;

/**
 * @author Kirill Kurdyukov
 */
public class ResultExtractor {

    private ResultExtractor() {
    }

    public static  <T> Mono<T> extract(Result<T> result, String failMessage) {
        if (result.isSuccess()) {
            return Mono.just(result.getValue());
        }

        return Mono.error(new UnexpectedResultException(failMessage, result.getStatus()));
    }

    public static  <T> Mono<T> extract(Result<T> result) {
        try {
            return Mono.just(result.getValue());
        } catch (UnexpectedResultException e) {
            return Mono.error(e);
        }
    }

    public static Mono<Void> extract(Status status) {
        try {
            status.expectSuccess();
            return Mono.empty();
        } catch (UnexpectedResultException e) {
            return Mono.error(e);
        }
    }
}
