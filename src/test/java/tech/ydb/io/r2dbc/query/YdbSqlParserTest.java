package tech.ydb.io.r2dbc.query;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @author Egor Kuleshov
 */
public class YdbSqlParserTest {

    static Stream<Object[]> sqlValues() {
        return Stream.of(
                new Object[]{"SELECT $1", new YdbQuery("SELECT $1", List.of(), QueryType.DML,
                        List.of(SqlOperation.SELECT))},
                new Object[]{"select $1", new YdbQuery("select $1", List.of(), QueryType.DML,
                        List.of(SqlOperation.SELECT))},
                new Object[]{"SELECT $1;", new YdbQuery("SELECT $1;", List.of(), QueryType.DML,
                        List.of(SqlOperation.SELECT))},
                new Object[]{"SELECT $1; Insert ?", new YdbQuery("SELECT $1; Insert $jp1", List.of("$jp1"),
                        QueryType.DML, List.of(SqlOperation.SELECT, SqlOperation.INSERT))},
                new Object[]{"INSERT $1", new YdbQuery("INSERT $1", List.of(), QueryType.DML,
                        List.of(SqlOperation.INSERT))},
                new Object[]{"SELECT ?", new YdbQuery("SELECT $jp1", List.of("$jp1"), QueryType.DML,
                        List.of(SqlOperation.SELECT))},
                new Object[]{"SELECT ? ?", new YdbQuery("SELECT $jp1 $jp2", List.of("$jp1", "$jp2"), QueryType.DML,
                        List.of(SqlOperation.SELECT))},
                new Object[]{"CREATE TABLE", new YdbQuery("CREATE TABLE", List.of(), QueryType.DDL,
                        List.of(SqlOperation.CREATE))},
                new Object[]{"DROP TABLE", new YdbQuery("DROP TABLE", List.of(), QueryType.DDL,
                        List.of(SqlOperation.DROP))});
    }

    @MethodSource("sqlValues")
    @ParameterizedTest
    void parserTest(String value, YdbQuery expected) {
        YdbQuery parsedQuery = YdbSqlParser.parse(value);

        Assertions.assertEquals(expected, parsedQuery);
    }

    @Test
    void parserTwoQueryTypesTest() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> YdbSqlParser.parse("SELECT ?; CREATE TABLE ?"),
                "DML and DDL don't support in one query");
    }
}
