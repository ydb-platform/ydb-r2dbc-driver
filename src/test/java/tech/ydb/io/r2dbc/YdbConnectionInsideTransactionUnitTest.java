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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.io.r2dbc.state.OutsideTransactionState;
import tech.ydb.io.r2dbc.state.CloseState;
import tech.ydb.io.r2dbc.state.InsideTransactionState;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.proto.ValueProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * @author Egor Kuleshov
 */
public class YdbConnectionInsideTransactionUnitTest {
    private static final YdbContext ydbContext = mock(YdbContext.class);
    private static final String txId = "test_tx_id";
    private static final YdbTxSettings ydbTxSettings = YdbTxSettings.defaultSettings().withAutoCommit(false);

    @BeforeEach
    public void init() {
        Mockito.when(ydbContext.getOperationsConfig()).thenReturn(OperationsConfig.defaultConfig());
    }

    @Test
    public void executeSchemeQueryTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any())).thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));

        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeSchemeQuery("test")
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session, never()).close();
    }

    @Test
    public void executeDataQueryTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .setTxMeta(YdbTable.TransactionMeta.newBuilder().setId(txId).build())
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));

        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(-1L)
                .verifyComplete();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session, never()).close();
    }

    @Test
    public void executeDataQueryCancelTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .setTxMeta(YdbTable.TransactionMeta.newBuilder().setId(txId).build())
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));

        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .thenCancel()
                .verify();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session, never()).close();
    }

    @Test
    public void executeDataQueryWithoutTxTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .setTxMeta(YdbTable.TransactionMeta.newBuilder().build())
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));

        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(-1L)
                .verifyComplete();

        Assertions.assertEquals(new OutsideTransactionState(ydbContext, ydbTxSettings),
                queryExecutor.getCurrentState());
        Mockito.verify(session).close();
    }

    @Test
    public void beginTransactionTest() {
        Session session = mock(Session.class);
        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.beginTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session, Mockito.never()).beginTransaction(any(), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void commitTransactionTest() {
        Session session = mock(Session.class);
        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.commitTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(new OutsideTransactionState(ydbContext, ydbTxSettings),
                queryExecutor.getCurrentState());
        Mockito.verify(session).commitTransaction(eq(txId), any());
        Mockito.verify(session).close();
    }

    @Test
    public void commitTransactionFailTest() {
        Session session = mock(Session.class);
        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.ABORTED)));
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.commitTransaction()
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session, never()).close();
    }

    @Test
    public void rollbackTransactionTest() {
        Session session = mock(Session.class);
        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        when(session.rollbackTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(new OutsideTransactionState(ydbContext, ydbTxSettings),
                queryExecutor.getCurrentState());
        Mockito.verify(session).rollbackTransaction(eq(txId), any());
        Mockito.verify(session).close();
    }

    @Test
    public void rollbackTransactionFailTest() {
        Session session = mock(Session.class);
        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        when(session.rollbackTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.ABORTED)));
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session, never()).close();
    }

    @Test
    public void setAutoCommitTrueTest() {
        Session session = mock(Session.class);
        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);
        when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));

        queryExecutor.setAutoCommit(true)
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(new OutsideTransactionState(ydbContext, ydbTxSettings.withAutoCommit(true)),
                queryExecutor.getCurrentState()
        );
        Mockito.verify(session).commitTransaction(eq(txId), any());
        Mockito.verify(session).close();
    }

    @Test
    public void setAutoCommitFalseTest() {
        Session session = mock(Session.class);
        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.setAutoCommit(false)
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session, never()).close();
    }

    @Test
    public void closeTest() {
        Session session = mock(Session.class);
        YdbConnectionState state = new InsideTransactionState(ydbContext, txId, session, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);
        when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));

        queryExecutor.close()
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(CloseState.INSTANCE, queryExecutor.getCurrentState());
        Mockito.verify(session).commitTransaction(eq(txId), any());
        Mockito.verify(session).close();
    }
}
