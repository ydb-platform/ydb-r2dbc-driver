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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.YdbIsolationLevel;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.table.Session;

/**
 * @author Egor Kuleshov
 */
public class InsideTransactionStateUnitTest {
    @Test
    public void getSessionTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings);

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

        YdbConnectionState state = new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings);

        YdbConnectionState newState = state.withDataQuery("test_tx_id", session);

        Assertions.assertEquals(state, newState);
    }

    @Test
    public void withDataQueryNonTransactionTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings);

        YdbConnectionState newState = state.withDataQuery("non_test_tx_id", session);

        Assertions.assertEquals(new OutsideTransactionState(ydbContext, ydbTxSettings), newState);
    }

    @Test
    public void withBeginTransactionTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        YdbTxSettings currentYdbTxSettings = Mockito.mock(YdbTxSettings.class);
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings);

        YdbConnectionState newState = state.withBeginTransaction("test_tx_id", session, currentYdbTxSettings);

        Assertions.assertEquals(state, newState);
    }

    @Test
    public void withCommitTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings);
        YdbConnectionState newState = state.withCommitTransaction();

        Assertions.assertInstanceOf(OutsideTransactionState.class, newState);
        Assertions.assertEquals(state.getYdbTxSettings(), newState.getYdbTxSettings());
        Mockito.verify(session).close();
    }

    @Test
    public void withRollbackTest() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings);
        YdbConnectionState newState = state.withRollbackTransaction();

        Assertions.assertInstanceOf(OutsideTransactionState.class, newState);
        Assertions.assertEquals(state.getYdbTxSettings(), newState.getYdbTxSettings());
        Mockito.verify(session).close();
    }

    @Test
    public void withAutoCommitTrue() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = new YdbTxSettings(
                YdbIsolationLevel.SERIALIZABLE,
                false,
                false
        );
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings);
        YdbConnectionState newState = state.withAutoCommit(true);

        Assertions.assertEquals(new OutsideTransactionState(ydbContext, ydbTxSettings.withAutoCommit(true)), newState);
    }

    @Test
    public void withAutoCommitFalse() {
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);
        Session session = Mockito.mock(Session.class);

        YdbConnectionState state = new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings);
        YdbConnectionState newState = state.withAutoCommit(false);

        Assertions.assertEquals(state, newState);
    }
}
