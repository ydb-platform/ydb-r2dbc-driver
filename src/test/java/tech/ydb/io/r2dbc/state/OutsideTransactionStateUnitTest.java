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

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import tech.ydb.core.Result;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.YdbIsolationLevel;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;

import static org.mockito.ArgumentMatchers.any;

/**
 * @author Egor Kuleshov
 */
public class OutsideTransactionStateUnitTest {

    @Test
    public void getSessionTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        TableClient tableClient = Mockito.mock(TableClient.class);
        Session session = Mockito.mock(Session.class);

        Mockito.when(ydbContext.getTableClient()).thenReturn(tableClient);
        Mockito.when(tableClient.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.getSession()
                .as(StepVerifier::create)
                .expectNext(session)
                .verifyComplete();
    }

    @Test
    public void withDataQueryTransactionTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        YdbConnectionState newState = state.withDataQuery("test_tx_id", session);

        Assertions.assertEquals(new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings.withAutoCommit(false)), newState);
    }

    @Test
    public void withDataQueryNonTransactionTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        YdbConnectionState newState = state.withDataQuery(null, session);

        Assertions.assertEquals(state, newState);
    }

    @Test
    public void withBeginTransactionTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        YdbTxSettings currentYdbTxSettings = Mockito.mock(YdbTxSettings.class);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        Session session = Mockito.mock(Session.class);
        YdbConnectionState newState = state.withBeginTransaction("test_tx_id", session, currentYdbTxSettings);

        Assertions.assertInstanceOf(InsideTransactionState.class, newState);
        Assertions.assertEquals(session, newState.getSession().block());
        Assertions.assertEquals(currentYdbTxSettings, newState.getYdbTxSettings());
        Assertions.assertTrue(newState.isInTransaction());
    }

    @Test
    public void withCommitTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnectionState newState = state.withCommitTransaction();

        Assertions.assertEquals(state, newState);
    }

    @Test
    public void withRollbackTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnectionState newState = state.withRollbackTransaction();

        Assertions.assertEquals(state, newState);
    }

    @Test
    public void withAutoCommitTrue() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = new YdbTxSettings(YdbIsolationLevel.SERIALIZABLE, false, true);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnectionState newState = state.withAutoCommit(true);

        Assertions.assertEquals(state, newState);
    }

    @Test
    public void withAutoCommitFalse() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = new YdbTxSettings(YdbIsolationLevel.SERIALIZABLE, false, true);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnectionState newState = state.withAutoCommit(false);

        Assertions.assertEquals(new OutsideTransactionState(ydbContext, ydbTxSettings.withAutoCommit(false)), newState);
    }
}
