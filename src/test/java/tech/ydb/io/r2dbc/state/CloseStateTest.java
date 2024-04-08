package tech.ydb.io.r2dbc.state;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import tech.ydb.core.Result;
import tech.ydb.io.r2dbc.YdbContext;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Egor Kuleshov
 */
public class CloseStateTest {
    private final TableClient client = mock(TableClient.class);
    private final YdbContext ydbContext = new YdbContext(client);

    @Test
    public void executeSchemaQueryTest() {
        TableClient client = mock(TableClient.class);
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any())).thenThrow(new RuntimeException());
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = CloseState.INSTANCE;
        QueryExecutor queryExecutor = new QueryExecutorImpl(ydbContext, state);

        queryExecutor.executeSchemaQuery("test")
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void executeDataQueryTest() {
        TableClient client = mock(TableClient.class);
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any())).thenThrow(new RuntimeException());
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = CloseState.INSTANCE;
        QueryExecutor queryExecutor = new QueryExecutorImpl(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.create(), List.of())
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }
}
