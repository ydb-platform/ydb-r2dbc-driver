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

import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.proto.ValueProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/**
 * @author Egor Kuleshov
 */
public class InsideTransactionStateUnitTest {
    private static final String TEST_QUERY = "testQuery";
    private static final String TEST_TX_ID = "test_tx_id";

    @Test
    public void executeDataQueryTest() {
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
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Params params = Mockito.mock(Params.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.executeDataQuery(TEST_QUERY, params, List.of(OperationType.SELECT))
                .as(StepVerifier::create)
                .expectNextMatches(fluxNextStateResult -> {
                    fluxNextStateResult.getResult()
                            .flatMap(YdbResult::getRowsUpdated)
                            .as(StepVerifier::create)
                            .expectNext(-1L)
                            .verifyComplete();

                    return fluxNextStateResult.getNextState().equals(state);
                })
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).executeDataQuery(eq(TEST_QUERY), any(), eq(params), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void executeDataQueryWithoutTxIdTest() {
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
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Params params = Mockito.mock(Params.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

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

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).executeDataQuery(eq(TEST_QUERY), any(), eq(params), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryFailTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.fail(Status.of(StatusCode.ABORTED))
        ));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Params params = Mockito.mock(Params.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.executeDataQuery(TEST_QUERY, params, List.of(OperationType.SELECT))
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).executeDataQuery(eq(TEST_QUERY), any(), eq(params), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void executeSchemeQueryTest() {
        Session session = Mockito.mock(Session.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.executeSchemeQuery(TEST_QUERY)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session, Mockito.never()).executeSchemeQuery(any(), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void beginTransactionTest() {
        Session session = Mockito.mock(Session.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.beginTransaction(ydbTxSettings)
                .as(StepVerifier::create)
                .expectNext(state)
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session, Mockito.never()).beginTransaction(any(), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void commitTransactionTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.commitTransaction()
                .as(StepVerifier::create)
                .expectNext(new OutsideTransactionState(ydbContext, ydbTxSettings))
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).commitTransaction(eq(TEST_TX_ID), any());
        Mockito.verify(session).close();
    }

    @Test
    public void commitTransactionFailTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.ABORTED)));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.commitTransaction()
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).commitTransaction(eq(TEST_TX_ID), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void rollbackTransactionTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.rollbackTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.rollbackTransaction()
                .as(StepVerifier::create)
                .expectNext(new OutsideTransactionState(ydbContext, ydbTxSettings))
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).rollbackTransaction(eq(TEST_TX_ID), any());
        Mockito.verify(session).close();
    }

    @Test
    public void rollbackTransactionFailTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.rollbackTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.ABORTED)));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).rollbackTransaction(eq(TEST_TX_ID), any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void setAutoCommitTrueTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.setAutoCommit(true)
                .as(StepVerifier::create)
                .expectNext(new OutsideTransactionState(ydbContext, ydbTxSettings))
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).commitTransaction(eq(TEST_TX_ID), any());
        Mockito.verify(ydbTxSettings).setAutoCommit(true);
        Mockito.verify(session).close();
    }

    @Test
    public void setAutoCommitTrueFailTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.ABORTED)));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.setAutoCommit(true)
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).commitTransaction(eq(TEST_TX_ID), any());
        Mockito.verify(ydbTxSettings, Mockito.never()).setAutoCommit(true);
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void setAutoCommitFalseTest() {
        Session session = Mockito.mock(Session.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.setAutoCommit(false)
                .as(StepVerifier::create)
                .expectNext(state)
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(ydbTxSettings, Mockito.never()).setAutoCommit(false);
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void setSameIsolationLevelTest() {
        Session session = Mockito.mock(Session.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        IsolationLevel isolationLevel = Mockito.mock(IsolationLevel.class);
        Mockito.when(ydbTxSettings.getIsolationLevel()).thenReturn(isolationLevel);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.setIsolationLevel(isolationLevel)
                .as(StepVerifier::create)
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(ydbTxSettings, Mockito.never()).setIsolationLevel(any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void setDiffIsolationLevelTest() {
        Session session = Mockito.mock(Session.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        IsolationLevel newIsolationLevel = Mockito.mock(IsolationLevel.class);
        IsolationLevel oldIsolationLevel = Mockito.mock(IsolationLevel.class);
        Mockito.when(ydbTxSettings.getIsolationLevel()).thenReturn(oldIsolationLevel);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.setIsolationLevel(newIsolationLevel)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(ydbTxSettings, Mockito.never()).setIsolationLevel(any());
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void setSameReadOnlyTest() {
        Session session = Mockito.mock(Session.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Mockito.when(ydbTxSettings.isReadOnly()).thenReturn(true);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.setReadOnly(true)
                .as(StepVerifier::create)
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(ydbTxSettings, Mockito.never()).setReadOnly(true);
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void setDiffReadOnlyTest() {
        Session session = Mockito.mock(Session.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Mockito.when(ydbTxSettings.isReadOnly()).thenReturn(false);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.setReadOnly(true)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(ydbTxSettings, Mockito.never()).setReadOnly(true);
        Mockito.verify(session, Mockito.never()).close();
    }

    @Test
    public void closeTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.close()
                .as(StepVerifier::create)
                .verifyComplete();

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).commitTransaction(eq(TEST_TX_ID), any());
        Mockito.verify(session).close();
    }

    @Test
    public void closeFailTest() {
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.commitTransaction(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.ABORTED)));
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        InsideTransactionState state = new InsideTransactionState(ydbContext, TEST_TX_ID, session, ydbTxSettings);

        state.close()
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Mockito.verify(ydbContext, Mockito.never()).getSession();
        Mockito.verify(session).commitTransaction(eq(TEST_TX_ID), any());
        Mockito.verify(session, Mockito.never()).close();
    }
}
