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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


import io.r2dbc.spi.R2dbcBadGrammarException;

import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseAlterKeyword;
import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseCreateKeyword;
import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseDeleteKeyword;
import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseDropKeyword;
import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseInsertKeyword;
import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseReplaceKeyword;
import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseSelectKeyword;
import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseUpdateKeyword;
import static tech.ydb.io.r2dbc.query.YdbParserUtils.parseUpsertKeyword;

/**
 * @author Egor Kuleshov
 */
public class YdbSqlParser {
    public static YdbQuery parse(String sql) {
        YdbQueryBuilder builder = new YdbQueryBuilder(sql);
        char[] chars = sql.toCharArray();
        int fragmentStart = 0;

        boolean nextExpression = true;

        for (int i = 0; i < chars.length; ++i) {
            char ch = chars[i];
            switch (ch) {
                case '\'' -> i = parseSingleQuotes(chars, i);
                case '"' -> i = parseDoubleQuotes(chars, i);
                case '-' -> i = parseLineComment(chars, i);
                case '/' -> i = parseBlockComment(chars, i);
                case ';' -> nextExpression = true;
                case '?' -> {
                    parseSpecialParam(chars, i, fragmentStart, builder);
                    fragmentStart = i + 1;
                }
                default -> {
                    if (nextExpression && Character.isJavaIdentifierStart(ch)) {
                        nextExpression = false;
                        builder.addExpression(parseExpression(chars, i));
                    }
                }
            }
        }


        if (fragmentStart < chars.length) {
            builder.append(chars, fragmentStart, chars.length - fragmentStart);
        }

        return builder.build();
    }

    private static void parseSpecialParam(char[] chars, int i, int prev, YdbQueryBuilder builder) {
        builder.append(chars, prev, i - prev);
        builder.addSpecialParameter();
    }

    private static ExpressionType parseExpression(final char[] chars, int i) {
        if (parseSelectKeyword(chars, i)) {
            return ExpressionType.SELECT;
        }

        if (isUpdateExpression(chars, i)) {
            return ExpressionType.UPDATE;
        }

        if (isSchemeExpression(chars, i)) {
            return ExpressionType.SCHEME;
        }

        throw new R2dbcBadGrammarException("Unexpected token at position " + i);
    }

    private static boolean isUpdateExpression(final char[] chars, int i) {
        return parseUpdateKeyword(chars, i)
                || parseInsertKeyword(chars, i)
                || parseUpsertKeyword(chars, i)
                || parseDeleteKeyword(chars, i)
                || parseReplaceKeyword(chars, i);
    }

    private static boolean isSchemeExpression(final char[] chars, int i) {
        return parseAlterKeyword(chars, i)
                || parseCreateKeyword(chars, i)
                || parseDropKeyword(chars, i);
    }

    private static int parseSingleQuotes(final char[] query, int offset) {
        while (++offset < query.length) {
            switch (query[offset]) {
                case '\\' -> ++offset;
                case '\'' -> {
                    return offset;
                }
                default -> {
                }
            }
        }

        return query.length;
    }

    private static int parseDoubleQuotes(final char[] query, int offset) {
        while (++offset < query.length && query[offset] != '"') {
            // do nothing
        }
        return offset;
    }

    private static int parseLineComment(final char[] query, int offset) {
        if (offset + 1 < query.length && query[offset + 1] == '-') {
            while (offset + 1 < query.length) {
                offset++;
                if (query[offset] == '\r' || query[offset] == '\n') {
                    break;
                }
            }
        }
        return offset;
    }

    private static int parseBlockComment(final char[] query, int offset) {
        if (offset + 1 < query.length && query[offset + 1] == '*') {
            // /* /* */ */ nest, according to SQL spec
            int level = 1;
            for (offset += 2; offset < query.length; ++offset) {
                switch (query[offset - 1]) {
                    case '*' -> {
                        if (query[offset] == '/') {
                            --level;
                            ++offset; // don't parse / in */* twice
                        }
                    }
                    case '/' -> {
                        if (query[offset] == '*') {
                            ++level;
                            ++offset; // don't parse * in /*/ twice
                        }
                    }
                    default -> {
                    }
                }

                if (level == 0) {
                    --offset; // reset position to last '/' char
                    break;
                }
            }
        }
        return offset;
    }

    private static class YdbQueryBuilder {
        private final String origin;
        private final StringBuilder query;
        private final List<String> args = new ArrayList<>();
        private final List<ExpressionType> expressions = new ArrayList<>();

        private int argsCounter = 0;
        private QueryType currentType = null;

        public YdbQueryBuilder(String origin) {
            this.origin = origin;
            this.query = new StringBuilder(origin.length() + 10);
        }

        public void addSpecialParameter() {
            while (true) {
                argsCounter++;
                String next = "$jp" + argsCounter;
                if (!origin.contains(next)) {
                    args.add(next);
                    append(next);

                    return;
                }
            }
        }

        public void addExpression(ExpressionType expressionType) {
            expressions.add(expressionType);

            if (currentType != null && currentType != expressionType.getQueryType()) {
                throw new UnsupportedOperationException("DML and DDL don't support in one query");
            }

            this.currentType = expressionType.getQueryType();
        }

        public void append(char[] chars, int start, int end) {
            query.append(chars, start, end);
        }

        public void append(char ch) {
            query.append(ch);
        }

        public void append(String string) {
            query.append(string);
        }

        public YdbQuery build() {
            Objects.requireNonNull(currentType);

            return new YdbQuery(query.toString(), args, currentType, expressions);
        }
    }
}
