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

package tech.ydb.io.r2dbc.result;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
 * @author Egor Kuleshov
 */
public class YdbResultUnitTest {
    @Test
    public void createTest() {
        ResultSetReader resultSetReader = Mockito.mock(ResultSetReader.class);
        Mockito.when(resultSetReader.getRowCount()).thenReturn(1);

        new YdbResult(resultSetReader);

        Mockito.verify(resultSetReader, Mockito.never()).getColumn(ArgumentMatchers.any());
    }

    @Test
    public void getRowsUpdatedTest() {
        ResultSetReader resultSetReader = Mockito.mock(ResultSetReader.class);
        Mockito.when(resultSetReader.getRowCount()).thenReturn(1);

        YdbResult ydbResult = new YdbResult(resultSetReader);
        ydbResult.getRowsUpdated()
                .as(StepVerifier::create)
                .expectNext(-1L)
                .verifyComplete();

        Mockito.verify(resultSetReader, Mockito.never()).getColumn(ArgumentMatchers.any());
    }

    @Test
    public void getRowTest() {
        ResultSetReader resultSetReader = Mockito.mock(ResultSetReader.class);
        ValueReader valueReader = Mockito.mock(ValueReader.class);
        Mockito.when(valueReader.getType()).thenReturn(PrimitiveType.Int32);
        Value value = PrimitiveValue.newInt32(123);
        Mockito.when(valueReader.getValue()).thenReturn(value);
        Mockito.when(resultSetReader.getRowCount()).thenReturn(1);
        Mockito.when(resultSetReader.getColumnCount()).thenReturn(1);
        Mockito.when(resultSetReader.getColumnName(0)).thenReturn("test");
        Mockito.when(resultSetReader.getColumnType(0)).thenReturn(PrimitiveType.Int32);
        Mockito.when(resultSetReader.getColumn(0)).thenReturn(valueReader);
        Mockito.when(resultSetReader.next()).thenReturn(true).thenReturn(false);

        YdbResult ydbResult = new YdbResult(resultSetReader);
        ydbResult.map((row, rowMetadata) -> row.get("test", Integer.class))
                .as(StepVerifier::create)
                .expectNext(123)
                .verifyComplete();
    }



    @Test
    public void getTwoRowsTest() {
        ResultSetReader resultSetReader = Mockito.mock(ResultSetReader.class);

        Mockito.when(resultSetReader.getRowCount()).thenReturn(2);
        Mockito.when(resultSetReader.getColumnCount()).thenReturn(1);
        Mockito.when(resultSetReader.getColumnName(0)).thenReturn("test");
        Mockito.when(resultSetReader.getColumnType(0)).thenReturn(PrimitiveType.Int32);

        ValueReader valueReader = Mockito.mock(ValueReader.class);
        Mockito.when(valueReader.getType()).thenReturn(PrimitiveType.Int32);
        Value value = PrimitiveValue.newInt32(123);
        Mockito.when(valueReader.getValue()).thenReturn(value);

        ValueReader valueReader2 = Mockito.mock(ValueReader.class);
        Mockito.when(valueReader2.getType()).thenReturn(PrimitiveType.Int32);
        Value value2 = PrimitiveValue.newInt32(124);
        Mockito.when(valueReader2.getValue()).thenReturn(value2);

        Mockito.when(resultSetReader.getColumn(0)).thenReturn(valueReader).thenReturn(valueReader2);
        Mockito.when(resultSetReader.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);

        YdbResult ydbResult = new YdbResult(resultSetReader);
        ydbResult.map((row, rowMetadata) -> row.get("test", Integer.class))
                .as(flux -> StepVerifier.create(flux, StepVerifierOptions.create().initialRequest(0)
                        .checkUnderRequesting(true)))
                .then(() -> Mockito.verify(resultSetReader, Mockito.times(0)).getColumn(0))
                .thenRequest(1)
                .then(() -> Mockito.verify(resultSetReader, Mockito.times(1)).getColumn(0))
                .expectNext(123)
                .then(() -> Mockito.verify(resultSetReader, Mockito.times(1)).getColumn(0))
                .thenRequest(1)
                .then(() -> Mockito.verify(resultSetReader, Mockito.times(2)).getColumn(0))
                .expectNext(124)
                .then(() -> Mockito.verify(resultSetReader, Mockito.times(2)).getColumn(0))
                .thenRequest(1)
                .verifyComplete();
    }
}
