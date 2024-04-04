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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.helper.R2dbcConnectionExtension;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 * @author Egor Kuleshov
 */
public class IntegrationTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private final R2dbcConnectionExtension r2dbc = new R2dbcConnectionExtension(ydb);

    @Test
    public void createAndDropTable() {
        r2dbc.connection().createStatement("create table t1 (id Int32, value Int32, primary key (id));")
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();

        r2dbc.connection().createStatement("drop table t1")
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    public void doubleInsertAndSelectTable() {
        r2dbc.connection().createStatement("create table t1 (id Int32, test_value TEXT, primary key (id));")
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();

        r2dbc.connection().createStatement("upsert into t1 (id, test_value) values (?, ?);")
                .bind(0, 123)
                .bind(1, "test_1")
                .add()
                .bind(0, 124)
                .bind(1, "test_2")
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1L)
                .expectNext(1L)
                .verifyComplete();

        r2dbc.connection().createStatement(
                        "select * from t1 order by id asc;" +
                                "select * from t1 order by id desc;")
                .execute()
                .flatMap(ydbResult -> ydbResult.map((row, rowMetadata) -> row.get("id")))
                .as(StepVerifier::create)
                .expectNext(123)
                .expectNext(124)
                .expectNext(124)
                .expectNext(123)
                .verifyComplete();

        r2dbc.connection().createStatement("drop table t1")
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
    }
}
