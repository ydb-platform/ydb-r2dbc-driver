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
public class ClientOptions {
    /**
     * Keep Query text
     */
    public static final Option<Boolean> KEEP_QUERY_TEXT = Option.valueOf("keepQueryText");
    /**
     * Session keep-alive timeout
     */
    public static final Option<Duration> SESSION_KEEP_ALIVE_TIME = Option.valueOf("sessionKeepAliveTime");
    /**
     * Session max idle time
     */
    public static final Option<Duration> SESSION_MAX_IDLE_TIME = Option.valueOf("sessionMaxIdleTime");
    /**
     * Session pool min size (with sessionPoolSizeMax)
     */
    public static final Option<Integer> SESSION_POOL_MIN_SIZE = Option.valueOf("sessionPoolMinSize");
    /**
     * Session pool max size (with sessionPoolSizeMin)
     */
    public static final Option<Integer> SESSION_POOL_MAX_SIZE = Option.valueOf("sessionPoolMaxSize");
}
