package tech.ydb.io.r2dbc.statement;

import java.util.List;
import java.util.Map;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.io.r2dbc.query.QueryType;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

import static org.mockito.Mockito.mock;

/**
 * @author kuleshovegor
 */
public class YdbDMLStatementTest {
    @Test
    public void bindNamedTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bind("$testParamA", 1);
        statement.bind("$testParamB", "test");

        Assertions.assertEquals(PrimitiveType.Text, statement.getBindings().getCurrent().values().get("$testParamB").getType());
        Assertions.assertEquals(Map.of("$testParamA", PrimitiveValue.newInt32(1),
                "$testParamB", PrimitiveValue.newText("test")),
                statement.getBindings().getCurrent().values());
    }

    @Test
    public void bindParameterTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bind("$testParamA", Parameters.in(R2dbcType.BIGINT, 1L));
        statement.bind("$testParamB", Parameters.in(R2dbcType.NVARCHAR,"test"));

        Assertions.assertEquals(PrimitiveType.Text, statement.getBindings().getCurrent().values().get("$testParamB").getType());
        Assertions.assertEquals(Map.of("$testParamA", PrimitiveValue.newInt64(1L),
                        "$testParamB", PrimitiveValue.newText("test")),
                statement.getBindings().getCurrent().values());
    }

    @Test
    public void bindIndexedTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bind(0, 1);
        statement.bind(1, "test");

        Assertions.assertEquals(Map.of("$testParamA", PrimitiveValue.newInt32(1),
                "$testParamB", PrimitiveValue.newText("test")),
                statement.getBindings().getCurrent().values());
    }

    @Test
    public void addBeforeFullBoundedTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParam"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        Assertions.assertThrows(IllegalArgumentException.class, statement::add);
    }

    @Test
    public void executeBeforeFullBoundedTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);
        statement.bind("$testParamA", "test");

        Assertions.assertThrows(IllegalArgumentException.class, statement::execute);
    }

    @Test
    public void bindNonExistTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParam1", "$testParam2"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        Assertions.assertThrows(IllegalArgumentException.class, () -> statement.bind("$testNonExistParam", 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> statement.bind(-1, 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> statement.bind(3, 1));
    }
}
