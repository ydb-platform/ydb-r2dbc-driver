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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.settings.YdbIsolationLevel;

/**
 * @author Egor Kuleshov
 */
public class TransactionIntegrationTest extends IntegrationBaseTest {
    @BeforeEach
    public void setUp() {
        createTable();
    }

    @AfterEach
    public void cleanUp() {
        r2dbc.connection().commitTransaction()
                .as(StepVerifier::create)
                .verifyComplete();
        r2dbc.connection().setTransactionIsolationLevel(YdbIsolationLevel.SERIALIZABLE)
                .as(StepVerifier::create)
                .verifyComplete();
        r2dbc.connection().setReadOnly(false)
                .as(StepVerifier::create)
                .verifyComplete();
        dropTable();
    }

    @Test
    public void beginTransactionWithDefinitionTest() {
        YdbConnection connection = r2dbc.connection();

        connection.beginTransaction(new YdbTransactionDefinition()
                        .isolationLevel(YdbIsolationLevel.SNAPSHOT_READ_ONLY)
                        .readOnly(true))
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(YdbIsolationLevel.SNAPSHOT_READ_ONLY, connection.getTransactionIsolationLevel());
    }

    @Test
    public void beginTransactionAndCommitTest() {
        YdbConnection connection = r2dbc.connection();

        connection.beginTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        simpleInsert(connection);

        oneSelect(connection);
        connection.commitTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        oneSelect(connection);
    }

    @Test
    public void beginTransactionAndSetAutoCommitTrueTest() {
        YdbConnection connection = r2dbc.connection();

        connection.beginTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        simpleInsert(connection);

        connection.setAutoCommit(true)
                .as(StepVerifier::create)
                .verifyComplete();

        oneSelect(connection);
    }

    @Test
    public void beginTransactionAndRollbackTest() {
        YdbConnection connection = r2dbc.connection();

        connection.beginTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        simpleInsert(connection);
        oneSelect(connection);

        connection.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        emptySelect(connection);
    }

    @Test
    public void setAutoCommitFalseAndRollbackTest() {
        YdbConnection connection = r2dbc.connection();

        connection.setAutoCommit(false)
                .as(StepVerifier::create)
                .verifyComplete();
        Assertions.assertFalse(connection.isAutoCommit());

        simpleInsert(connection);
        oneSelect(connection);

        connection.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        emptySelect(connection);
    }

    @Test
    public void beginTransactionSetIsolationLevelTest() {
        YdbConnection connection = r2dbc.connection();

        connection.beginTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        connection.setTransactionIsolationLevel(YdbIsolationLevel.SNAPSHOT_READ_ONLY)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void beginTransactionSetSameIsolationLevelTest() {
        YdbConnection connection = r2dbc.connection();

        connection.beginTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        connection.setTransactionIsolationLevel(connection.getTransactionIsolationLevel())
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Test
    public void setIsolationLevelTest() {
        YdbConnection connection = r2dbc.connection();

        Assertions.assertEquals(YdbIsolationLevel.SERIALIZABLE, connection.getTransactionIsolationLevel());

        connection.setTransactionIsolationLevel(YdbIsolationLevel.SNAPSHOT_READ_ONLY)
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(YdbIsolationLevel.SNAPSHOT_READ_ONLY, connection.getTransactionIsolationLevel());
    }

    @Test
    public void setReadOnlyFalseTest() {
        YdbConnection connection = r2dbc.connection();

        Assertions.assertEquals(YdbIsolationLevel.SERIALIZABLE, connection.getTransactionIsolationLevel());

        connection.setReadOnly(false)
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertFalse(connection.isReadOnly());
    }

    @Test
    public void setReadOnlyFalseFailTest() {
        YdbConnection connection = r2dbc.connection();

        connection.setTransactionIsolationLevel(YdbIsolationLevel.SNAPSHOT_READ_ONLY)
                .as(StepVerifier::create)
                .verifyComplete();

        connection.setReadOnly(false)
                .as(StepVerifier::create)
                .verifyError(IllegalArgumentException.class);
    }

    private static void simpleInsert(YdbConnection connection) {
        connection.createStatement("insert into t1 (id, value) values (?, ?);")
                .bind(0, 123)
                .bind(1, "test_1")
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1L)
                .verifyComplete();
    }

    private static void emptySelect(YdbConnection connection) {
        connection.createStatement("select * from t1 order by id;")
                .execute()
                .flatMap(ydbResult -> ydbResult.map((row, rowMetadata) -> row.get("id")))
                .as(StepVerifier::create)
                .verifyComplete();
    }

    private static void oneSelect(YdbConnection connection) {
        connection.createStatement(
                        "select * from t1 order by id;")
                .execute()
                .flatMap(ydbResult -> ydbResult.map((row, rowMetadata) -> row.get("id")))
                .as(StepVerifier::create)
                .expectNext(123)
                .verifyComplete();
    }
}
