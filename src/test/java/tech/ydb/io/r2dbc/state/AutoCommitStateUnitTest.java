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
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Egor Kuleshov
 */
public class AutoCommitStateUnitTest {
    private final TableClient client = mock(TableClient.class);
    private final YdbContext ydbContext = new YdbContext(client);

    @Test
    public void executeSchemaQueryTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any())).thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new AutoCommitState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        QueryExecutor queryExecutor = new QueryExecutorImpl(ydbContext, state);

        queryExecutor.executeSchemaQuery("test")
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    public void createSessionErrorTest() {
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.fail(Status.of(StatusCode.CANCELLED))));

        YdbConnectionState state = new AutoCommitState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        QueryExecutor queryExecutor = new QueryExecutorImpl(ydbContext, state);

        queryExecutor.executeSchemaQuery("test")
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);
    }

    @Test
    public void executeSchemaQueryFailTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any())).thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.BAD_REQUEST)));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new AutoCommitState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        QueryExecutor queryExecutor = new QueryExecutorImpl(ydbContext, state);

        queryExecutor.executeSchemaQuery("test")
                .map(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);
    }

    @Test
    public void executeSchemaQueryErrorTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any())).thenThrow(new RuntimeException());
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new AutoCommitState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        QueryExecutor queryExecutor = new QueryExecutorImpl(ydbContext, state);

        queryExecutor.executeSchemaQuery("test")
                .as(StepVerifier::create)
                .verifyError(RuntimeException.class);
    }
}
