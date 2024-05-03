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
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.state.CloseState;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.table.query.Params;

/**
 * @author Egor Kuleshov
 */
public class YdbConnectionCloseStateTest {

    @Test
    public void executeSchemaQueryTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(state);

        queryExecutor.executeSchemaQuery("test")
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void executeDataQueryTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of())
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void beginTransactionTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(state);

        queryExecutor.beginTransaction()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void commitTransactionTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(state);

        queryExecutor.commitTransaction()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void rollbackTransactionTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(state);

        queryExecutor.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setAutoCommitTrueTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(state);

        queryExecutor.setAutoCommit(true)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setAutoCommitFalseTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(state);

        queryExecutor.setAutoCommit(false)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void closeTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        YdbConnection queryExecutor = new YdbConnection(state);

        queryExecutor.close()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }
}
