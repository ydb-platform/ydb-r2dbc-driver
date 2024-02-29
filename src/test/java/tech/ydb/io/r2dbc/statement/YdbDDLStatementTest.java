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

package tech.ydb.io.r2dbc.statement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.YdbConnectionState;

import static org.mockito.Mockito.mock;

/**
 * @author kuleshovegor
 */
public class YdbDDLStatementTest {
    @Test
    public void testAdd() {
        YdbQuery query = mock(YdbQuery.class);
        YdbConnectionState ydbConnectionState = mock(YdbConnectionState.class);

        YdbStatement statement = new YdbDDLStatement(query, ydbConnectionState);

        Assertions.assertThrows(UnsupportedOperationException.class,
                statement::add);
    }

    @Test
    public void testBind() {
        YdbQuery query = mock(YdbQuery.class);
        YdbConnectionState ydbConnectionState = mock(YdbConnectionState.class);

        YdbStatement statement = new YdbDDLStatement(query, ydbConnectionState);

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> statement.bind(0, 123));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> statement.bind("$testName", 123));
    }

    @Test
    public void testBindNull() {
        YdbQuery query = mock(YdbQuery.class);
        YdbConnectionState ydbConnectionState = mock(YdbConnectionState.class);

        YdbStatement statement = new YdbDDLStatement(query, ydbConnectionState);

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> statement.bindNull(0, Integer.class));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> statement.bindNull("$testName", Integer.class));
    }
}
