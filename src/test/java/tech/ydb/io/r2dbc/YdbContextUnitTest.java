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

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import tech.ydb.io.r2dbc.options.OperationOptions;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.query.YdbSqlParser;
import tech.ydb.table.impl.PooledTableClient;

/**
 * @author Egor Kuleshov
 */
public class YdbContextUnitTest {
    private final YdbQuery ydbQuery = Mockito.mock(YdbQuery.class);

    @Test
    public void statementCacheTest() {
        try (MockedStatic<YdbSqlParser> parser = Mockito.mockStatic(YdbSqlParser.class)) {
            parser.when(() -> YdbSqlParser.parse("test"))
                    .thenReturn(ydbQuery);

            YdbContext ydbContext = new YdbContext(
                    Mockito.mock(PooledTableClient.class),
                    new OperationsConfig(new OptionExtractor(ConnectionFactoryOptions.builder()
                            .option(OperationOptions.STATEMENT_CACHE_SIZE, 1)
                            .build()))
            );

            ydbContext.findOrParseYdbQuery("test");
            ydbContext.findOrParseYdbQuery("test");

            parser.verify(() -> YdbSqlParser.parse("test"));
        }
    }

    @Test
    public void statementWithoutCacheTest() {
        try (MockedStatic<YdbSqlParser> parser = Mockito.mockStatic(YdbSqlParser.class)) {
            parser.when(() -> YdbSqlParser.parse("test"))
                    .thenReturn(ydbQuery);

            YdbContext ydbContext = new YdbContext(
                    Mockito.mock(PooledTableClient.class),
                    new OperationsConfig(new OptionExtractor(ConnectionFactoryOptions.builder()
                            .option(OperationOptions.STATEMENT_CACHE_SIZE, 0)
                            .build()))
            );

            ydbContext.findOrParseYdbQuery("test");
            ydbContext.findOrParseYdbQuery("test");

            parser.verify(() -> YdbSqlParser.parse("test"), Mockito.times(2));
        }
    }

    @Test
    public void statementCacheMissTest() {
        YdbQuery ydbQuery2 = Mockito.mock(YdbQuery.class);

        try (MockedStatic<YdbSqlParser> parser = Mockito.mockStatic(YdbSqlParser.class)) {
            parser.when(() -> YdbSqlParser.parse("test"))
                    .thenReturn(ydbQuery);
            parser.when(() -> YdbSqlParser.parse("test2"))
                    .thenReturn(ydbQuery2);

            YdbContext ydbContext = new YdbContext(
                    Mockito.mock(PooledTableClient.class),
                    new OperationsConfig(new OptionExtractor(ConnectionFactoryOptions.builder()
                            .option(OperationOptions.STATEMENT_CACHE_SIZE, 1)
                            .build()))
            );

            ydbContext.findOrParseYdbQuery("test");
            ydbContext.findOrParseYdbQuery("test");
            ydbContext.findOrParseYdbQuery("test2");
            ydbContext.findOrParseYdbQuery("test2");

            parser.verify(() -> YdbSqlParser.parse("test"), Mockito.times(1));
            parser.verify(() -> YdbSqlParser.parse("test"), Mockito.times(1));
        }
    }
}
