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
                        List.of(ExpressionType.SELECT))});
    }

    @MethodSource("sqlValues")
    @ParameterizedTest
    public void parserTest(String sql, YdbQuery expected) {
        Assertions.assertEquals(expected, YdbSqlParser.parse(sql));
    }
}
