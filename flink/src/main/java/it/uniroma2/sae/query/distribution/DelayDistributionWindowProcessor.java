package it.uniroma2.sae.query.distribution;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serial;
import java.util.Iterator;

/**
 * Process-window function used for finite event-time delay-distribution windows.
 * <p>
 * The heavy aggregation work is performed incrementally by {@link DelayDistributionAggregator}.
 * This processor receives the already aggregated {@link DelayDistributionAccumulator}, attaches
 * the concrete {@link TimeWindow} boundaries, and emits the final {@link DelayDistributionResult}.
 * </p>
 * <p>
 * The emitted window end timestamp follows Flink's standard time-window convention: start is
 * inclusive and end is exclusive.
 * </p>
 */
public class DelayDistributionWindowProcessor
        extends ProcessWindowFunction<DelayDistributionAccumulator, DelayDistributionResult, Tuple2<String, Integer>, TimeWindow> {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Logical output label associated with the window scope, for example {@code "1d"} or {@code "7d"}. */
    private final String windowType;

    /**
     * Creates a processor for a specific finite window scope.
     *
     * @param windowType the label written into each emitted result to identify the window family
     */
    public DelayDistributionWindowProcessor(String windowType) {
        this.windowType = windowType;
    }

    /**
     * Emits one enriched result for the current key and window firing.
     *
     * @param key the composite key containing airline code and scheduled departure hour
     * @param ctx the Flink window context exposing the current time-window metadata
     * @param accumulators the iterable containing the aggregated accumulator produced by the aggregate function
     * @param out the collector receiving the final delay-distribution result
     */
    @Override
    public void process(
            Tuple2<String, Integer> key,
            Context ctx,
            Iterable<DelayDistributionAccumulator> accumulators,
            Collector<DelayDistributionResult> out) {

        Iterator<DelayDistributionAccumulator> iterator = accumulators.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        DelayDistributionAccumulator acc = iterator.next();

        out.collect(DelayDistributionResult.fromAccumulator(
                ctx.window().getStart(),
                ctx.window().getEnd(),
                windowType,
                key,
                acc
        ));
    }
}