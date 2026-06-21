package it.uniroma2.sae.query.distribution;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Window processor for 1d and 7d windows in DelayDistributionQuery.
 * Extracts percentiles from the sketch accumulator and emits the result.
 */
public class DelayDistributionWindowProcessor 
        extends ProcessWindowFunction<DelayDistributionAccumulator, DelayDistributionResult, Tuple2<String, Integer>, TimeWindow> {

    private final String windowType;

    public DelayDistributionWindowProcessor(String windowType) {
        this.windowType = windowType;
    }

    @Override
    public void process(
            Tuple2<String, Integer> key,
            Context ctx,
            Iterable<DelayDistributionAccumulator> accumulators,
            Collector<DelayDistributionResult> out) {

        DelayDistributionAccumulator acc = accumulators.iterator().next();
        long windowStart = ctx.window().getStart();
        long windowEnd   = ctx.window().getEnd();

        out.collect(new DelayDistributionResult(
                windowStart,
                windowEnd,
                windowType,
                key.f0, // airline
                key.f1, // hour
                acc.getCount(),
                acc.getMin(),
                acc.getPercentile(0.25),
                acc.getPercentile(0.50),
                acc.getPercentile(0.75),
                acc.getPercentile(0.90),
                acc.getMax()
        ));
    }
}
