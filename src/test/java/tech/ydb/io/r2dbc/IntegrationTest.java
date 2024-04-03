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

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;
import tech.ydb.core.Status;
import tech.ydb.io.r2dbc.result.YdbDDLResult;
import tech.ydb.io.r2dbc.state.OutTransaction;
import tech.ydb.table.TableClient;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 * @author Egor Kuleshov
 */
public class IntegrationTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @Test
    public void test() {
        TableClient tableClient = TableClient.newClient(ydb.createTransport())
                .sessionPoolSize(1, 2)
                .build();

        OutTransaction outTransaction = new OutTransaction(tableClient, TxControl.serializableRw(), Duration.ofSeconds(1));

        outTransaction.executeSchemaQuery("create table t1 (id Int32, value Int32, primary key (id));")
                .map(YdbDDLResult::getStatus)
                .map(Status::getCode)
                .as(StepVerifier::create)
                .expectNext(Status.SUCCESS.getCode())
                .verifyComplete();

        outTransaction.executeSchemaQuery("drop table t1;")
                .map(YdbDDLResult::getStatus)
                .map(Status::getCode)
                .as(StepVerifier::create)
                .expectNext(Status.SUCCESS.getCode())
                .verifyComplete();
    }
}
