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


import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import tech.ydb.table.query.Params;
import tech.ydb.table.values.Value;

/**
 * @author Egor Kuleshov
 */
public class YdbQuery {
    private final String yqlQuery;
    private final QueryType type;
    private final List<String> indexesArgsNames;

    YdbQuery(String yql, List<String> indexesArgsNames, QueryType queryType) {
        this.yqlQuery = yql;
        this.indexesArgsNames = indexesArgsNames;
        this.type = queryType;
    }

    public String getYqlQuery(Params params) throws SQLException {
        StringBuilder yql = new StringBuilder();

        if (indexesArgsNames != null) {
            if (params != null) {
                Map<String, Value<?>> values = params.values();
                for (String prm : indexesArgsNames) {
                    if (!values.containsKey(prm)) {
                        throw new SQLDataException("Missing value for parameter: " + prm);
                    }

                    String prmType = values.get(prm).getType().toString();
                    yql.append("DECLARE ")
                            .append(prm)
                            .append(" AS ")
                            .append(prmType)
                            .append(";\n");


                }
            } else if (!indexesArgsNames.isEmpty()) {
                yql.append("-- DECLARE ").append(indexesArgsNames.size()).append(" PARAMETERS").append("\n");
            }
        }

        yql.append(yqlQuery);
        return yql.toString();
    }

    public QueryType type() {
        return type;
    }
}
