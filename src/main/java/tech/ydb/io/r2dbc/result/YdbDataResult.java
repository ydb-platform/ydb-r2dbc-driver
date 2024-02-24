package tech.ydb.io.r2dbc.result;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import tech.ydb.table.query.DataQueryResult;

public class YdbDataResult implements Result {
    public YdbDataResult(DataQueryResult dataQueryResult) {
        this.dataQueryResult = dataQueryResult;
    }

    private final DataQueryResult dataQueryResult;



    @Override
    public Publisher<Long> getRowsUpdated() {
        return null;
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> biFunction) {
        return null;
    }

    @Override
    public Result filter(Predicate<Segment> predicate) {
        return null;
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> function) {
        return null;
    }
}
