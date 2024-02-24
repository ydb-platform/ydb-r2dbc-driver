package tech.ydb.io.r2dbc.context;

/**
 * @author kuleshovegor
 */
public final class YdbConst {
    public static final String MULTI_TYPES_IN_ONE_QUERY = "Query cannot contain expressions with different types: ";
    public static final String MISSING_VALUE_FOR_PARAMETER = "Missing value for parameter: ";
    public static final String VARIABLE_PARAMETER_PREFIX = "$";
    public static final String AUTO_GENERATED_PARAMETER_PREFIX = VARIABLE_PARAMETER_PREFIX + "jp";

    private YdbConst() {
        //
    }
}
