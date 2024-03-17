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

package tech.ydb.io.r2dbc.query;

/**
 * @author Egor Kuleshov
 */
enum SqlOperation {
    ALTER("alter", OperationType.SCHEME),
    CREATE("create", OperationType.SCHEME),
    DROP("drop", OperationType.SCHEME),
    SELECT("select", OperationType.SELECT),
    UPDATE("update", OperationType.UPDATE),
    UPSERT("upsert", OperationType.UPDATE),
    INSERT("insert", OperationType.UPDATE),
    DELETE("delete", OperationType.UPDATE),
    REPLACE("replace", OperationType.UPDATE);

    private final char[] keyword;
    private final OperationType operationType;

    SqlOperation(String keyword, OperationType operationType) {
        this.keyword = keyword.toCharArray();
        this.operationType = operationType;
    }

    public char[] getKeyword() {
        return keyword;
    }

    public OperationType getOperationType() {
        return operationType;
    }
}
