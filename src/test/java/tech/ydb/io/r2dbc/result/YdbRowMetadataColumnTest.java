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
import org.mockito.Mockito;
import tech.ydb.table.values.Type;

/**
 * @author Egor Kuleshov
 */
public class YdbRowMetadataColumnTest {
    @Test
    public void getColumnMetadataTest() {
        Type type1 = Mockito.mock(Type.class);
        Type type2 = Mockito.mock(Type.class);
        YdbColumnMetadata ydbColumnMetadata1 = new YdbColumnMetadata(type1, "test1");
        YdbColumnMetadata ydbColumnMetadata2 = new YdbColumnMetadata(type2, "test2");
        YdbRowMetadata ydbRowMetadata = new YdbRowMetadata(List.of(
                ydbColumnMetadata1,
                ydbColumnMetadata2
        ));

        Assertions.assertEquals(ydbColumnMetadata1, ydbRowMetadata.getColumnMetadata("test1"));
        Assertions.assertEquals(ydbColumnMetadata2, ydbRowMetadata.getColumnMetadata("test2"));
        Assertions.assertEquals(ydbColumnMetadata1, ydbRowMetadata.getColumnMetadata(0));
        Assertions.assertEquals(ydbColumnMetadata2, ydbRowMetadata.getColumnMetadata(1));

        Assertions.assertEquals(List.of(ydbColumnMetadata1, ydbColumnMetadata2), ydbRowMetadata.getColumnMetadatas());
    }

    @Test
    public void getColumnMetadataNonExistTest() {
        Type type = Mockito.mock(Type.class);
        YdbColumnMetadata ydbColumnMetadata = new YdbColumnMetadata(type, "test");
        YdbRowMetadata ydbRowMetadata = new YdbRowMetadata(List.of(ydbColumnMetadata));

        Assertions.assertThrows(IllegalArgumentException.class, () -> ydbRowMetadata.getColumnMetadata("NonExist"));
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> ydbRowMetadata.getColumnMetadata(-1));
    }
}
