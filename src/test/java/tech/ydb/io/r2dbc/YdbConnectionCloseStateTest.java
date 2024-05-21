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
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.state.CloseState;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.table.query.Params;

/**
 * @author Egor Kuleshov
 */
public class YdbConnectionCloseStateTest {
    private final YdbContext ydbContext = Mockito.mock(YdbContext.class);

    @Test
    public void executeSchemeQueryTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeSchemeQuery("test")
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void executeDataQueryTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of())
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void beginTransactionTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.beginTransaction()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void commitTransactionTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.commitTransaction()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void rollbackTransactionTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setAutoCommitTrueTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.setAutoCommit(true)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setAutoCommitFalseTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.setAutoCommit(false)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void closeTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.close()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }
}
