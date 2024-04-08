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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import io.r2dbc.spi.Result;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.parameter.YdbParameter;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.type.YdbType;

/**
 * @author Egor Kuleshov
 */
public class TypesIntegrationTest extends IntegrationBaseTest {
    static Stream<Object[]> values() {
        return Stream.of(
                new Object[]{YdbType.BOOL, true},
                new Object[]{YdbType.INT8, (byte) 1},
                new Object[]{YdbType.INT16, (short) 1},
                new Object[]{YdbType.INT32, 123},
                new Object[]{YdbType.INT64, 123L},
                new Object[]{YdbType.FLOAT, (float) 1.5},
                new Object[]{YdbType.DOUBLE, 1.5},
                new Object[]{YdbType.TEXT, "test"},
                new Object[]{YdbType.DATE, LocalDate.ofYearDay(2024, 1)},
                new Object[]{YdbType.DATETIME, LocalDateTime.of(2024, 2, 3, 4, 5, 6)},
                new Object[]{YdbType.TIMESTAMP, Instant.ofEpochMilli(12345)},
                new Object[]{YdbType.INTERVAL, Duration.ofSeconds(1)}
        );
    }

    @ParameterizedTest(name = "with {0}")
    @EnumSource(value = YdbType.class, mode = EnumSource.Mode.EXCLUDE, names = {
            "UUID",
            "TZ_DATE",
            "TZ_DATETIME",
            "TZ_TIMESTAMP"})
    public void createInsertSelectNull(YdbType type) {
        r2dbc.connection().createStatement("create table t1_" + type + " (id INT32, test_value " + type.getYdbType() + ", primary key (id));")
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();

        r2dbc.connection().createStatement("UPSERT INTO t1_" + type + " (id, test_value) values (1, ?);")
                .bind(0, new YdbParameter(type))
                .execute()
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1L)
                .verifyComplete();

        r2dbc.connection().createStatement("SELECT * from t1_" + type)
                .execute()
                .flatMap(ydbResult ->
                        ydbResult.map((row, rowMetadata) -> row.get("test_value", type.getJavaType()) == null))
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        r2dbc.connection().createStatement("drop table t1_" + type)
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
    }

    @ParameterizedTest(name = "with {0}")
    @MethodSource("values")
    public void createInsertSelectValue(YdbType type, Object value) {
        r2dbc.connection().createStatement("create table t1_" + type + " (id INT32, test_value " + type.getYdbType() + ", primary key (id));")
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();

        r2dbc.connection().createStatement("UPSERT INTO t1_" + type + " (id, test_value) values (1, ?);")
                .bind(0, value)
                .execute()
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1L)
                .verifyComplete();

        r2dbc.connection().createStatement("SELECT * from t1_" + type)
                .execute()
                .flatMap(result -> result.map((row, rowMetadata) -> (Object) row.get("test_value", value.getClass())))
                .as(StepVerifier::create)
                .expectNext(value)
                .verifyComplete();

        r2dbc.connection().createStatement("drop table t1_" + type)
                .execute()
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
    }
}
