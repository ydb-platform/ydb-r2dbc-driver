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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.query.QueryType;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.statement.YdbDDLStatement;
import tech.ydb.io.r2dbc.statement.YdbDMLStatement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/**
 * @author Egor Kuleshov
 */
public class YdbBatchUnitTest {
    @Test
    public void singleTest() {
        YdbConnection ydbConnection = Mockito.mock(YdbConnection.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbQuery query = new YdbQuery("test", List.of(), QueryType.DML);

        Mockito.when(ydbConnection.createStatement(any(YdbQuery.class)))
                .thenReturn(new YdbDMLStatement(query, ydbConnection));
        YdbResult ydbResult = Mockito.mock(YdbResult.class);
        Mockito.when(ydbResult.getRowsUpdated())
                .thenReturn(Mono.just(-1L));
        Mockito.when(ydbContext.findOrParseYdbQuery(Mockito.any()))
                .thenReturn(query);
        Mockito.when(ydbConnection.executeDataQuery(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Flux.just(ydbResult));

        YdbBatch batch = new YdbBatch(ydbConnection, ydbContext);

        batch.add("test");

        batch.execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(-1L)
                .verifyComplete();

        Mockito.verify(ydbContext).findOrParseYdbQuery("test");
        Mockito.verify(ydbConnection).executeDataQuery(eq("test"), any(), any());
    }

    @Test
    public void doubleTest() {
        YdbConnection ydbConnection = Mockito.mock(YdbConnection.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbQuery query = new YdbQuery("test1;\ntest2", List.of(), QueryType.DML);

        Mockito.when(ydbConnection.createStatement(any(YdbQuery.class)))
                .thenReturn(new YdbDMLStatement(query, ydbConnection));
        YdbResult ydbResult = Mockito.mock(YdbResult.class);
        Mockito.when(ydbResult.getRowsUpdated())
                .thenReturn(Mono.just(-1L));
        Mockito.when(ydbContext.findOrParseYdbQuery(Mockito.any()))
                .thenReturn(query);
        Mockito.when(ydbConnection.executeDataQuery(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Flux.just(ydbResult));

        YdbBatch batch = new YdbBatch(ydbConnection, ydbContext);

        batch.add("test1").add("test2");

        batch.execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(-1L)
                .verifyComplete();

        Mockito.verify(ydbContext).findOrParseYdbQuery("test1;\ntest2");
        Mockito.verify(ydbConnection).executeDataQuery(eq("test1;\ntest2"), any(), any());
    }

    @Test
    public void schemeTest() {
        YdbConnection ydbConnection = Mockito.mock(YdbConnection.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        YdbQuery query = new YdbQuery("test1;\ntest2", List.of(), QueryType.DDL);
        Mockito.when(ydbConnection.createStatement(any(YdbQuery.class)))
                .thenReturn(new YdbDDLStatement(query, ydbConnection));

        Mockito.when(ydbContext.findOrParseYdbQuery(Mockito.any())).thenReturn(query);
        YdbResult ydbResult = Mockito.mock(YdbResult.class);
        Mockito.when(ydbResult.getRowsUpdated()).thenReturn(Mono.just(-1L));
        Mockito.when(ydbConnection.executeSchemeQuery(Mockito.any()))
                .thenReturn(Flux.just(ydbResult));

        YdbBatch batch = new YdbBatch(ydbConnection, ydbContext);

        batch.add("test1").add("test2");

        batch.execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(-1L)
                .verifyComplete();

        Mockito.verify(ydbContext).findOrParseYdbQuery("test1;\ntest2");
        Mockito.verify(ydbConnection, Mockito.never()).executeDataQuery(any(), any(), any());
    }

    @Test
    public void parametersExceptionTest() {
        YdbConnection ydbConnection = Mockito.mock(YdbConnection.class);
        YdbContext ydbContext = Mockito.mock(YdbContext.class);
        Mockito.when(ydbContext.findOrParseYdbQuery(Mockito.any()))
                .thenReturn(new YdbQuery("test1;\ntest2", List.of("test"), QueryType.DML));

        YdbBatch batch = new YdbBatch(ydbConnection, ydbContext);

        batch.add("test1").add("test2");

        batch.execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError(IllegalArgumentException.class);

        Mockito.verify(ydbContext).findOrParseYdbQuery("test1;\ntest2");
        Mockito.verify(ydbConnection, Mockito.never()).executeDataQuery(any(), any(), any());
    }
}
