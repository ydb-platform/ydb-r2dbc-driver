package tech.ydb.io.r2dbc.state;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.YdbIsolationLevel;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.table.query.Params;

/**
 * @author Egor Kuleshov
 */
public class CloseStateUnitTest {
    private static final String TEST_QUERY = "testQuery";
    private static final YdbConnectionState state = CloseState.INSTANCE;
    @Test
    public void executeDataQueryTest() {
        state.executeDataQuery(TEST_QUERY, Params.empty(), List.of())
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void executeSchemeQueryTest() {
        state.executeSchemeQuery(TEST_QUERY)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void beginTransactionTest() {
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        state.beginTransaction(ydbTxSettings)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void commitTransactionTest() {
        state.commitTransaction()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void rollbackTransactionTest() {
        state.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setAutoCommitTrueTest() {
        state.setAutoCommit(true)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setAutoCommitFalseTest() {
        state.setAutoCommit(false)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setIsolationLevelTest() {
        YdbIsolationLevel isolationLevel = Mockito.mock(YdbIsolationLevel.class);

        state.setIsolationLevel(isolationLevel)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setReadOnlyTest() {
        state.setReadOnly(true)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void setStatementTimeoutTest() {
        Duration timeout = Mockito.mock(Duration.class);

        state.setStatementTimeout(timeout)
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void getYdbTxSettings() {
        Assertions.assertThrows(IllegalStateException.class, state::getYdbTxSettings);
    }

    @Test
    public void closeTest() {
        state.close()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }
}
