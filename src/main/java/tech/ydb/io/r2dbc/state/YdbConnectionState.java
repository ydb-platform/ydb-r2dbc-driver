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
import java.util.List;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tech.ydb.io.r2dbc.YdbIsolationLevel;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.table.query.Params;

/**
 * The YDB connection state provides methods for changing the state, making requests, and getting a new state.
 *
 * @author Egor Kuleshov
 */
public interface YdbConnectionState {
    /**
     * Execute a data query to YDB, returns the result and the next connection state.
     *
     * @param yql built a query with the specified types of parameters
     * @param params query parameters
     * @param operationTypes types of queries within a single yql query
     * @return the result of the yql query is wrapped in the NextStateResult with the next state
     */
    Mono<NextStateResult<Flux<YdbResult>>> executeDataQuery(String yql, Params params, List<OperationType> operationTypes);

    /**
     * Execute a schema query to YDB, returns the result.
     *
     * @param yql built a query without parameters
     * @return the result of the yql schema query
     */
    Flux<YdbResult> executeSchemaQuery(String yql);

    /**
     * Begin a transaction with the settings if the transaction has not been started yet, otherwise it does nothing.
     *
     * @param ydbTxSettings settings for the beginning of the transaction
     * @return connection state in the transaction
     */
    Mono<InsideTransactionState> beginTransaction(YdbTxSettings ydbTxSettings);

    /**
     * Commit the transaction if there is an open transaction, otherwise it does nothing.
     *
     * @return connection state out the transaction
     */
    Mono<OutsideTransactionState> commitTransaction();

    /**
     * Rollback the transaction if there is an open transaction, otherwise it does nothing.
     *
     * @return connection state out the transaction
     */
    Mono<OutsideTransactionState> rollbackTransaction();

    /**
     * Set auto-commit mode. If autoCommit is true and there is an open transaction, then it will be committed.
     * Calling this method without changing auto-commit mode this invocation results in a no-op.
     *
     * @param autoCommit auto-commit mode
     * @return new connection state
     */
    Mono<YdbConnectionState> setAutoCommit(boolean autoCommit);

    /**
     * Set isolation level for next transactions.
     * Change current state.
     * Calling this method without changing auto-commit mode this invocation results in a no-op.
     * Error if there is an open transaction.
     *
     * @param isolationLevel ydb isolation level of transactions.
     * @return {@link Mono} that indicates that a transaction level has been configured
     * or Mono.error() if there is an open transaction.
     */
    Mono<Void> setIsolationLevel(YdbIsolationLevel isolationLevel);

    /**
     * Set isolation level for next transactions.
     * Change current state.
     * Calling this method without changing auto-commit mode this invocation results in a no-op.
     * Error if there is an open transaction.
     *
     * @param readOnly read only mode of transactions.
     * @return {@link Mono} that indicates that a read only mode configured
     * or Mono.error() if there is an open transaction.
     */
    Mono<Void> setReadOnly(boolean readOnly);

    /**
     * Configures the statement timeout for statements to be executed using the current connection.
     * Change current state.
     *
     * @param timeout the statement timeout for this connection. {@link Duration#ZERO} indicates no timeout.
     * @return a {@link Mono} that indicates that a statement timeout has been configured.
     */
    Mono<Void> setStatementTimeout(Duration timeout);

    /**
     * Get transaction settings.
     *
     * @return current transaction settings.
     */
    YdbTxSettings getYdbTxSettings();

    /**
     * Close state. If there is an open transaction, then it will be committed.
     *
     * @return Mono that indicates that connection state closed.
     */
    Mono<Void> close();
}
