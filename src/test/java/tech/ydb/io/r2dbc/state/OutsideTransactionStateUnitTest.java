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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.io.r2dbc.OperationsConfig;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.proto.ValueProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.impl.PooledTableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.Transaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/**
 * @author Egor Kuleshov
 */
public class OutsideTransactionStateUnitTest {
    private static final String TEST_QUERY = "testQuery";
    private static final String TEST_TX_ID = "test_tx_id";

    @Test
    public void executeDataQueryTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Params params = Mockito.mock(Params.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.executeDataQuery(TEST_QUERY, params, List.of(OperationType.SELECT))
                .as(StepVerifier::create)
                .expectNextMatches(fluxNextStateResult -> {
                    fluxNextStateResult.getResult()
                            .flatMap(YdbResult::getRowsUpdated)
                            .as(StepVerifier::create)
                            .expectNext(-1L)
                            .verifyComplete();

                    return fluxNextStateResult.getNextState()
                            .equals(new OutsideTransactionState(ydbContext, ydbTxSettings));
                })
                .verifyComplete();

        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).executeDataQuery(eq(TEST_QUERY), any(), eq(params), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryWithTxIdTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .setTxMeta(YdbTable.TransactionMeta.newBuilder()
                                                .setId(TEST_TX_ID)
                                                .build())
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Params params = Mockito.mock(Params.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.executeDataQuery(TEST_QUERY, params, List.of(OperationType.SELECT))
                .as(StepVerifier::create)
                .expectNextMatches(fluxNextStateResult -> {
                    fluxNextStateResult.getResult()
                            .flatMap(YdbResult::getRowsUpdated)
                            .as(StepVerifier::create)
                            .expectNext(-1L)
                            .verifyComplete();

                    return fluxNextStateResult.getNextState()
                            .equals(new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings
                                    .withAutoCommit(false)));
                })
                .verifyComplete();

        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).executeDataQuery(eq(TEST_QUERY), any(), eq(params), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void executeDataQueryFailTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Params params = Mockito.mock(Params.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.executeDataQuery(TEST_QUERY, params, List.of(OperationType.SELECT))
                .as(StepVerifier::create)
                .thenCancel()
                .verify();

        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).executeDataQuery(eq(TEST_QUERY), any(), eq(params), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryCancelTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.fail(Status.of(StatusCode.ABORTED))
        ));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Params params = Mockito.mock(Params.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.executeDataQuery(TEST_QUERY, params, List.of(OperationType.SELECT))
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).executeDataQuery(eq(TEST_QUERY), any(), eq(params), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeSchemeQueryTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.executeSchemeQuery(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.executeSchemeQuery(TEST_QUERY)
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).executeSchemeQuery(eq(TEST_QUERY), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeSchemeQueryFailTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.executeSchemeQuery(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.ABORTED)));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.executeSchemeQuery(TEST_QUERY)
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).executeSchemeQuery(eq(TEST_QUERY), any());
        Mockito.verify(session).close();
    }

    @Test
    public void beginTransactionTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Transaction transaction = Mockito.mock(Transaction.class);
        Mockito.when(transaction.getId()).thenReturn(TEST_TX_ID);
        Mockito.when(session.beginTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(transaction)));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Transaction.Mode mode = Mockito.mock(Transaction.Mode.class);
        Mockito.when(ydbTxSettings.getMode()).thenReturn(mode);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.beginTransaction(ydbTxSettings)
                .as(StepVerifier::create)
                .expectNext(new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings))
                .verifyComplete();

        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).beginTransaction(eq(mode), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void beginTransactionFailTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Transaction transaction = Mockito.mock(Transaction.class);
        Mockito.when(transaction.getId()).thenReturn(TEST_TX_ID);
        Mockito.when(session.beginTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Result.fail(Status.of(StatusCode.ABORTED))));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Transaction.Mode mode = Mockito.mock(Transaction.Mode.class);
        Mockito.when(ydbTxSettings.getMode()).thenReturn(mode);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.beginTransaction(ydbTxSettings)
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).beginTransaction(eq(mode), any());
        Mockito.verify(session).close();
    }

    @Test
    public void commitTransactionTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.commitTransaction()
                .as(StepVerifier::create)
                .expectNext(state)
                .verifyComplete();

        Mockito.verify(tableClient, Mockito.never()).createSession(any());
    }

    @Test
    public void rollbackTransactionTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.rollbackTransaction()
                .as(StepVerifier::create)
                .expectNext(state)
                .verifyComplete();

        Mockito.verify(tableClient, Mockito.never()).createSession(any());
    }

    @Test
    public void setAutoCommitTrueTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = YdbTxSettings.defaultSettings();

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.setAutoCommit(true)
                .as(StepVerifier::create)
                .expectNext(state)
                .verifyComplete();

        Assertions.assertEquals(ydbTxSettings, state.getYdbTxSettings());
        Assertions.assertTrue(ydbTxSettings.isAutoCommit());
    }

    @Test
    public void setAutoCommitFalseTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.setAutoCommit(false)
                .as(StepVerifier::create)
                .expectNext(state)
                .verifyComplete();

        Mockito.verify(ydbTxSettings).setAutoCommit(false);
    }

    @Test
    public void keepAliveLocalTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.keepAlive(ValidationDepth.LOCAL)
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        Mockito.verify(tableClient, Mockito.never()).createSession(any());
    }

    @Test
    public void keepAliveRemoteTrueTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.keepAlive(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(Session.State.READY)));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.keepAlive(ValidationDepth.REMOTE)
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();


        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).keepAlive(any());
        Mockito.verify(session).close();
    }

    @Test
    public void keepAliveRemoteFalseTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.keepAlive(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(Session.State.BUSY)));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.keepAlive(ValidationDepth.REMOTE)
                .as(StepVerifier::create)
                .expectNext(false)
                .verifyComplete();


        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).keepAlive(any());
        Mockito.verify(session).close();
    }

    @Test
    public void keepAliveRemoteFailTest() {
        PooledTableClient tableClient = Mockito.mock(PooledTableClient.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.keepAlive(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.fail(Status.of(StatusCode.ABORTED))));
        Mockito.when(tableClient.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbContext ydbContext = new YdbContext(tableClient, OperationsConfig.defaultConfig());
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.keepAlive(ValidationDepth.REMOTE)
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);


        Mockito.verify(tableClient).createSession(any());
        Mockito.verify(session).keepAlive(any());
        Mockito.verify(session).close();
    }
}
