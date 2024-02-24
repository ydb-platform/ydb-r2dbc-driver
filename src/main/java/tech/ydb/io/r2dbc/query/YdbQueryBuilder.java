package tech.ydb.io.r2dbc.query;

import java.util.ArrayList;
import java.util.List;

import io.r2dbc.spi.R2dbcBadGrammarException;
import tech.ydb.io.r2dbc.context.YdbConst;

/**
 * @author kuleshovegor
 */
public class YdbQueryBuilder {
    private final String origin;
    private final StringBuilder query;
    private final List<String> args = new ArrayList<>();
    private final List<YdbExpression> expressions = new ArrayList<>();

    private int argsCounter = 0;
    private QueryType currentType = null;

    public YdbQueryBuilder(String origin) {
        this.origin = origin;
        this.query = new StringBuilder(origin.length() + 10);
    }

    public String createNextArgName() {
        while (true) {
            argsCounter += 1;
            String next = YdbConst.AUTO_GENERATED_PARAMETER_PREFIX + argsCounter;
            if (!origin.contains(next)) {
                args.add(next);
                return next;
            }
        }
    }

    public void addExpression(QueryType type, YdbExpression expression) throws R2dbcBadGrammarException {
        expressions.add(expression);

        if (currentType != null && currentType != type) {
            throw new R2dbcBadGrammarException(YdbConst.MULTI_TYPES_IN_ONE_QUERY + currentType + ", " + type);
        }
        this.currentType = type;
    }

    public QueryType getQueryType() {

        if (currentType != null) {
            return currentType;
        }

        return QueryType.DATA_QUERY;
    }

    public List<YdbExpression> getExpressions() {
        return expressions;
    }

    public String getOriginSQL() {
        return origin;
    }

    public String buildYQL() {
        return query.toString();
    }

    public List<String> getIndexedArgs() {
        return args;
    }

    public void append(char[] chars, int start, int end) {
        query.append(chars, start, end);
    }

    public void append(char ch) {
        query.append(ch);
    }

    public void append(String string) {
        query.append(string);
    }

    public YdbQuery build() {
        return new YdbQuery(this);
    }
}
