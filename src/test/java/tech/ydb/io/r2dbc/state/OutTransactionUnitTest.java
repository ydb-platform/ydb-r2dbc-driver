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

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.io.r2dbc.result.YdbDDLResult;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Egor Kuleshov
 */
public class OutTransactionUnitTest {
    @Test
    public void executeSchemaQueryTest() {
        TableClient client = mock(TableClient.class);
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any())).thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutTransaction(client, null, null);
        state.executeSchemaQuery("test")
                .map(YdbDDLResult::getStatus)
                .as(StepVerifier::create)
                .expectNext(Status.SUCCESS)
                .verifyComplete();
    }

    @Test
    public void createSessionErrorTest() {
        TableClient client = mock(TableClient.class);
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.fail(Status.of(StatusCode.CANCELLED))));

        YdbConnectionState state = new OutTransaction(client, null, null);
        state.executeSchemaQuery("test")
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);
    }

    @Test
    public void executeSchemaQueryFailTest() {
        TableClient client = mock(TableClient.class);
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.BAD_REQUEST)));
        when(client.createSession(any()))
                .thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutTransaction(client, null, null);
        state.executeSchemaQuery("test")
                .map(YdbDDLResult::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);
    }

    @Test
    public void executeSchemaQueryErrorTest() {
        TableClient client = mock(TableClient.class);
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any())).thenThrow(new RuntimeException());
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutTransaction(client, null, null);
        state.executeSchemaQuery("test")
                .as(StepVerifier::create)
                .verifyError(RuntimeException.class);
    }
}
