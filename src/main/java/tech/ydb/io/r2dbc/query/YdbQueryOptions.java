package tech.ydb.io.r2dbc.query;


/**
 * @author kuleshovegor
 */
public class YdbQueryOptions {
    private final boolean isDetectQueryType;
    private final boolean isDetectJdbcParameters;
    private final boolean isDeclareJdbcParameters;

    YdbQueryOptions(
            boolean detectQueryType,
            boolean detectJbdcParams,
            boolean declareJdbcParams
    ) {
        this.isDetectQueryType = detectQueryType;
        this.isDetectJdbcParameters = detectJbdcParams;
        this.isDeclareJdbcParameters = declareJdbcParams;
    }

    public boolean isDetectQueryType() {
        return isDetectQueryType;
    }

    public boolean isDetectJdbcParameters() {
        return isDetectJdbcParameters;
    }

    public boolean isDeclareJdbcParameters() {
        return isDeclareJdbcParameters;
    }
}
