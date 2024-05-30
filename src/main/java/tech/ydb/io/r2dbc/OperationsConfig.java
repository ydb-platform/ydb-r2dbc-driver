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

import java.time.Duration;

import io.r2dbc.spi.ConnectionFactoryOptions;
import tech.ydb.io.r2dbc.options.OperationOptions;

/**
 * @author Egor Kuleshov
 */
public class OperationsConfig {
    private static final Duration DEFAULT_STATEMENT_TIMEOUT = Duration.ZERO;
    private static final boolean DEFAULT_FAIL_ON_TRUNCATED_RESULT = false;
    private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_DEADLINE_TIMEOUT = Duration.ZERO;
    private static final int DEFAULT_STATEMENT_CACHE_SIZE = 256;

    private final Duration statementTimeout;
    private final boolean failOnTruncatedResult;
    private final Duration sessionTimeout;
    private final Duration deadlineTimeout;
    private final int statementCacheSize;

    public OperationsConfig(OptionExtractor optionExtractor) {
        this.statementTimeout = optionExtractor.extractOrDefault(ConnectionFactoryOptions.STATEMENT_TIMEOUT,
                DEFAULT_STATEMENT_TIMEOUT);
        this.failOnTruncatedResult = optionExtractor.extractOrDefault(OperationOptions.FAIL_ON_TRUNCATED_RESULT,
                DEFAULT_FAIL_ON_TRUNCATED_RESULT);
        this.sessionTimeout = optionExtractor.extractOrDefault(OperationOptions.SESSION_TIMEOUT,
                DEFAULT_SESSION_TIMEOUT);
        this.deadlineTimeout = optionExtractor.extractOrDefault(OperationOptions.DEADLINE_TIMEOUT,
                DEFAULT_DEADLINE_TIMEOUT);
        this.statementCacheSize = optionExtractor.extractOrDefault(OperationOptions.STATEMENT_CACHE_SIZE,
                DEFAULT_STATEMENT_CACHE_SIZE);
    }

    public static OperationsConfig defaultConfig() {
        return new OperationsConfig(OptionExtractor.empty());
    }

    public Duration getStatementTimeout() {
        return statementTimeout;
    }

    public Boolean getFailOnTruncatedResult() {
        return failOnTruncatedResult;
    }

    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    public Duration getDeadlineTimeout() {
        return deadlineTimeout;
    }

    public int getStatementCacheSize() {
        return statementCacheSize;
    }
}
