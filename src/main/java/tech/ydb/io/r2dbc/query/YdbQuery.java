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

import java.sql.SQLException;
import java.util.List;

import tech.ydb.io.r2dbc.statement.binding.Binding;

/**
 * @author Egor Kuleshov
 */
public class YdbQuery {
    private final String yqlQuery;
    private final QueryType type;
    private final List<String> indexesArgsNames;

    public YdbQuery(String yql, List<String> indexesArgsNames, QueryType queryType) {
        this.yqlQuery = yql;
        this.indexesArgsNames = indexesArgsNames;
        this.type = queryType;
    }

    public String getYqlQuery(Binding binding) throws SQLException {
        return getDeclares(binding) + yqlQuery;
    }

    public static String getDeclares(Binding binding) {
        StringBuilder yql = new StringBuilder();
        binding.values().forEach((name, value) -> yql.append("DECLARE ")
                .append(name)
                .append(" AS ")
                .append(value.getType().toString())
                .append(";\n"));

        return yql.toString();
    }

    public List<String> getIndexArgNames() {
        return indexesArgsNames;
    }

    public QueryType type() {
        return type;
    }
}
