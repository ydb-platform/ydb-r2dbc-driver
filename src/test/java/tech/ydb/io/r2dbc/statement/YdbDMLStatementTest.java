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
import tech.ydb.io.r2dbc.type.YdbType;
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
        statement.bind("$testParamB", Parameters.in(R2dbcType.NVARCHAR, "test"));

        Assertions.assertEquals(Map.of("$testParamA", PrimitiveValue.newInt64(1L),
                        "$testParamB", PrimitiveValue.newText("test")),
                statement.getBindings().getCurrent().values());
    }

    @Test
    public void bindYdbParameterTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bind("$testParamA", Parameters.in(YdbType.INT32, 1));
        statement.bind("$testParamB", Parameters.in(YdbType.JSON, "test"));

        Assertions.assertEquals(Map.of("$testParamA", PrimitiveValue.newInt32(1),
                        "$testParamB", PrimitiveValue.newJson("test")),
                statement.getBindings().getCurrent().values());
    }

    @Test
    public void bindClassParameterTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bind("$testParamA", Parameters.in(1));
        statement.bind("$testParamB", Parameters.in("test"));

        Assertions.assertEquals(Map.of("$testParamA", PrimitiveValue.newInt32(1),
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
    public void bindNullTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bindNull(0, int.class);
        statement.bindNull("$testParamB", String.class);

        Assertions.assertEquals(Map.of("$testParamA", PrimitiveType.Int32.makeOptional().emptyValue(),
                        "$testParamB",PrimitiveType.Text.makeOptional().emptyValue()),
                statement.getBindings().getCurrent().values());
    }

    @Test
    public void bindNullParameterTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bind("$testParamA", Parameters.in(R2dbcType.BIGINT));
        statement.bind("$testParamB", Parameters.in(R2dbcType.NVARCHAR));

        Assertions.assertEquals(Map.of("$testParamA", PrimitiveType.Int64.makeOptional().emptyValue(),
                        "$testParamB", PrimitiveType.Text.makeOptional().emptyValue()),
                statement.getBindings().getCurrent().values());
    }

    @Test
    public void bindNullYdbParameterTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA", "$testParamB"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bind("$testParamA", Parameters.in(YdbType.INT32));
        statement.bind("$testParamB", Parameters.in(YdbType.JSON));

        Assertions.assertEquals(Map.of("$testParamA", PrimitiveType.Int32.makeOptional().emptyValue(),
                        "$testParamB", PrimitiveType.Json.makeOptional().emptyValue()),
                statement.getBindings().getCurrent().values());
    }

    @Test
    public void bindNullClassParameterTest() {
        YdbQuery query = new YdbQuery("test_sql", List.of("$testParamA"), QueryType.DML);
        YdbConnectionState connectionState = mock(YdbConnectionState.class);
        YdbStatement statement = new YdbDMLStatement(query, connectionState);

        statement.bind("$testParamA", Parameters.in(String.class));

        Assertions.assertEquals(Map.of("$testParamA", PrimitiveType.Text.makeOptional().emptyValue()),
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
