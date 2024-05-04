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

package tech.ydb.io.r2dbc.state;

import java.time.Duration;

import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.table.settings.RequestSettings;

/**
 * @author Egor Kuleshov
 */
public abstract class AbstractConnectionState implements YdbConnectionState {
    protected final YdbContext ydbContext;
    protected volatile YdbTxSettings ydbTxSettings;
    protected volatile Duration statementTimeout;

    public AbstractConnectionState(YdbContext ydbContext, YdbTxSettings ydbTxSettings, Duration statementTimeout) {
        this.ydbContext = ydbContext;
        this.ydbTxSettings = ydbTxSettings;
        this.statementTimeout = statementTimeout;
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        this.statementTimeout = timeout;

        return Mono.empty();
    }

    @Override
    public YdbTxSettings getYdbTxSettings() {
        return ydbTxSettings;
    }

    protected  <T extends RequestSettings<?>> T withStatementTimeout(T settings) {
        if (!statementTimeout.isZero() && !statementTimeout.isNegative()) {
            settings.setOperationTimeout(statementTimeout);
            settings.setTimeout(statementTimeout.plusSeconds(1));
        }

        return settings;
    }
}
