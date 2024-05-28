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

package tech.ydb.io.r2dbc.options;

import java.time.Duration;

import io.r2dbc.spi.Option;

/**
 * @author Egor Kuleshov
 */
public class OperationOptions {
    /**
     * Return an exception when received truncated result
     */
    public static final Option<Boolean> FAIL_ON_TRUNCATED_RESULT = Option.valueOf("failOnTruncatedResult");
    /**
     * Default timeout to create a session
     */
    public static final Option<Duration> SESSION_TIMEOUT = Option.valueOf("sessionTimeout");
    /**
     * Deadline timeout for all operations
     */
    public static final Option<Duration> DEADLINE_TIMEOUT = Option.valueOf("deadlineTimeout");
    /**
     * Specifies the maximum number of entries in per-transport cache of parsed statements.
     * A value of {@code 0} disables the cache.
     */
    public static final Option<Integer> STATEMENT_CACHE_SIZE = Option.valueOf("statementCacheQueries");
}
