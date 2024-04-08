package tech.ydb.io.r2dbc.state;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.YdbTxSettings;
import tech.ydb.table.Session;

/**
 * @author Egor Kuleshov
 */
public class CloseStateUnitTest {
    @Test
    public void getSessionTest() {
        YdbConnectionState state = CloseState.INSTANCE;

        state.getSession()
                .as(StepVerifier::create)
                .verifyError(IllegalStateException.class);
    }

    @Test
    public void withDataQueryTest() {
        YdbTxSettings currentYdbTxSettings = Mockito.mock(YdbTxSettings.class);

        YdbConnectionState state = CloseState.INSTANCE;

        Session session = Mockito.mock(Session.class);

        Assertions.assertThrows(IllegalStateException.class, () -> state
                .withBeginTransaction("test_tx_id", session, currentYdbTxSettings));
    }

    @Test
    public void withBeginTransactionTest() {
        YdbTxSettings currentYdbTxSettings = Mockito.mock(YdbTxSettings.class);

        YdbConnectionState state = CloseState.INSTANCE;

        Session session = Mockito.mock(Session.class);

        Assertions.assertThrows(IllegalStateException.class, () -> state
                .withBeginTransaction("test_tx_id", session, currentYdbTxSettings));
    }

    @Test
    public void withCommitTest() {
        YdbConnectionState state = CloseState.INSTANCE;
        Assertions.assertThrows(IllegalStateException.class, state::withCommitTransaction);
    }

    @Test
    public void withRollbackTest() {
        YdbConnectionState state = CloseState.INSTANCE;

        Assertions.assertThrows(IllegalStateException.class, state::withRollbackTransaction);
    }

    @Test
    public void withAutoCommitTrue() {
        YdbConnectionState state = CloseState.INSTANCE;

        Assertions.assertThrows(IllegalStateException.class, () -> state.withAutoCommit(true));
    }

    @Test
    public void withAutoCommitFalse() {
        YdbConnectionState state = CloseState.INSTANCE;

        Assertions.assertThrows(IllegalStateException.class, () -> state.withAutoCommit(false));
    }
}
