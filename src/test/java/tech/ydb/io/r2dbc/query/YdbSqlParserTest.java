package tech.ydb.io.r2dbc.query;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @author kuleshovegor
 */
public class YdbSqlParserTest {

    static Stream<Object[]> sqlValues() {
        return Stream.of(
                new Object[]{"SELECT $1", new YdbQuery("SELECT $1", List.of(), QueryType.DML,
                        List.of(ExpressionType.SELECT))},
                new Object[]{"SELECT $1;", new YdbQuery("SELECT $1", List.of(), QueryType.DML,
                        List.of(ExpressionType.SELECT))},
                new Object[]{"Insert $1;", new YdbQuery("Insert $1", List.of(), QueryType.DML,
                        List.of(ExpressionType.SELECT))},
                new Object[]{"SELECT ?;", new YdbQuery("SELECT $1", List.of("$jp1"), QueryType.DML,
                        List.of(ExpressionType.SELECT))},
                new Object[]{"SELECT ?? ?;", new YdbQuery("SELECT $1", List.of("$jp1", "$jp2"), QueryType.DML,
                        List.of(ExpressionType.SELECT))});
    }

    @MethodSource("sqlValues")
    @ParameterizedTest
    void parserTest(String value, YdbQuery expected) {
        YdbQuery parsedQuery = YdbSqlParser.parse(value);

        Assertions.assertEquals(expected.getExpressionTypes(), parsedQuery.getExpressionTypes());
        Assertions.assertEquals(expected.getIndexesArgsNames(), parsedQuery.getIndexesArgsNames());
        Assertions.assertEquals(expected.type(), parsedQuery.type());
    }
}
