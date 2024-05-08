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

package tech.ydb.io.r2dbc.result;

import io.r2dbc.spi.Nullability;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import tech.ydb.io.r2dbc.type.YdbType;
import tech.ydb.table.values.Type;

/**
 * @author Egor Kuleshov
 */
public class YdbColumnMetadataUnitTest {
    private static final String TEST_COLUMN_NAME = "testName";

    @Test
    public void getNativeTypeMetadataTest() {
        Type type = Mockito.mock(Type.class);

        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(type, TEST_COLUMN_NAME);

        Assertions.assertEquals(type, ydbColumnMetadata.getNativeTypeMetadata());
    }

    @Test
    public void getNameTest() {
        Type type = Mockito.mock(Type.class);

        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(type, TEST_COLUMN_NAME);

        Assertions.assertEquals(TEST_COLUMN_NAME, ydbColumnMetadata.getName());
    }

    @ParameterizedTest
    @EnumSource(YdbType.class)
    public void getJavaType(YdbType ydbType) {
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(ydbType.getYdbType(), TEST_COLUMN_NAME);

        Assertions.assertEquals(ydbType.getJavaType(), ydbColumnMetadata.getJavaType());
    }

    @ParameterizedTest
    @EnumSource(YdbType.class)
    public void getNullabilityNonNullTest(YdbType ydbType) {
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(ydbType.getYdbType(), TEST_COLUMN_NAME);

        Assertions.assertEquals(Nullability.NON_NULL, ydbColumnMetadata.getNullability());
    }

    @ParameterizedTest
    @EnumSource(YdbType.class)
    public void getNullabilityNullableTest(YdbType ydbType) {
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(
                ydbType.getYdbType().makeOptional(),
                TEST_COLUMN_NAME
        );

        Assertions.assertEquals(Nullability.NULLABLE, ydbColumnMetadata.getNullability());
    }
}
