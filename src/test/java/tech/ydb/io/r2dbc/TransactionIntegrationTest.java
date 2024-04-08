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
        dropTable();
    }
    @Test
    public void beginTransactionAndCommit() {
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
    public void beginTransactionAndAutoCommitTrue() {
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
    public void beginTransactionAndRollback() {
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
    public void setAutoCommitFalseAndCommit() {
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
    public void beginTransactionSetIsolationLevel() {
        YdbConnection connection = r2dbc.connection();

        connection.beginTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        connection.setYdbTransactionIsolationLevel(YdbIsolationLevel.SNAPSHOT_READ_ONLY)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
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
