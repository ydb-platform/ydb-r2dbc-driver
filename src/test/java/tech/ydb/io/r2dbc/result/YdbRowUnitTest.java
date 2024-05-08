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

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.io.r2dbc.type.YdbType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveValue;

/**
 * @author Egor Kuleshov
 */
public class YdbRowUnitTest {
    @Test
    public void getTest() {
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(YdbType.INT32.getYdbType(), "test");
        YdbRowMetadata ydbRowMetadata = new YdbRowMetadata(List.of(ydbColumnMetadata));
        YdbRow ydbRow =  new YdbRow(ydbRowMetadata, List.of(PrimitiveValue.newInt32(123)));

        Assertions.assertEquals(123, ydbRow.get("test"));
        Assertions.assertEquals(123, ydbRow.get(0));
        Assertions.assertEquals(123, ydbRow.get("test", Integer.class));
    }

    @Test
    public void getOptionalTest() {
        OptionalType type = YdbType.INT32.getYdbType().makeOptional();
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(type, "test");
        YdbRowMetadata ydbRowMetadata = new YdbRowMetadata(List.of(ydbColumnMetadata));
        YdbRow ydbRow =  new YdbRow(ydbRowMetadata, List.of(type.newValue(PrimitiveValue.newInt32(123))));

        Assertions.assertEquals(123, ydbRow.get("test"));
        Assertions.assertEquals(123, ydbRow.get(0));
        Assertions.assertEquals(123, ydbRow.get("test", Integer.class));
    }

    @Test
    public void getNullTest() {
        OptionalType type = YdbType.INT32.getYdbType().makeOptional();
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(type, "test");
        YdbRowMetadata ydbRowMetadata = new YdbRowMetadata(List.of(ydbColumnMetadata));
        YdbRow ydbRow =  new YdbRow(ydbRowMetadata, List.of(type.emptyValue()));

        Assertions.assertNull(ydbRow.get("test"));
        Assertions.assertNull(ydbRow.get(0));
        Assertions.assertNull(ydbRow.get("test", Integer.class));
    }

    @Test
    public void getNonExistTest() {
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(YdbType.INT32.getYdbType(), "test");
        YdbRowMetadata ydbRowMetadata = new YdbRowMetadata(List.of(ydbColumnMetadata));
        YdbRow ydbRow =  new YdbRow(ydbRowMetadata, List.of(PrimitiveValue.newInt32(123)));

        Assertions.assertThrows(IllegalArgumentException.class, () -> ydbRow.get("notexits"));
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> ydbRow.get(-1));
    }

    @Test
    public void getWrongTypeTest() {
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(YdbType.INT32.getYdbType(), "test");
        YdbRowMetadata ydbRowMetadata = new YdbRowMetadata(List.of(ydbColumnMetadata));
        YdbRow ydbRow =  new YdbRow(ydbRowMetadata, List.of(PrimitiveValue.newInt32(123)));

        Assertions.assertThrows(ClassCastException.class, () -> ydbRow.get("test", String.class));
    }
}
