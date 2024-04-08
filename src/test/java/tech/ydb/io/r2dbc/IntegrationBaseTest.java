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

import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.helper.R2dbcConnectionExtension;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 * @author Egor Kuleshov
 */
public abstract class IntegrationBaseTest {
    private static final String CREATE_TABLE = "create table t1 (id Int32, value text, primary key (id));";
    private static final String DROP_TABLE = "drop table t1;";
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    protected final R2dbcConnectionExtension r2dbc = new R2dbcConnectionExtension(ydb);

    protected void createTable() {
        r2dbc.connection().createStatement(CREATE_TABLE)
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
    }

    protected void dropTable() {
        r2dbc.connection().createStatement(DROP_TABLE)
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
    }
}
