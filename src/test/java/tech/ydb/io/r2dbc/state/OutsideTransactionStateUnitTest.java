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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import tech.ydb.core.Result;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.proto.ValueProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;

import static org.mockito.ArgumentMatchers.any;

/**
 * @author Egor Kuleshov
 */
public class OutsideTransactionStateUnitTest {
    private static final String TEST_QUERY = "testQuery";
    private static final String TEST_TX_ID = "test_tx_id";

    @Test
    public void executeDataQueryTest() {
        TableClient tableClient = Mockito.mock(TableClient.class);
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
        YdbContext ydbContext = new YdbContext(tableClient);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.executeDataQuery(TEST_QUERY, Params.empty(), List.of(OperationType.SELECT))
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

        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryWithTxIdTest() {
        TableClient tableClient = Mockito.mock(TableClient.class);
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
        YdbContext ydbContext = new YdbContext(tableClient);
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        OutsideTransactionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);

        state.executeDataQuery(TEST_QUERY, Params.empty(), List.of(OperationType.SELECT))
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

        Mockito.verify(session, Mockito.never()).close();
    }
}
