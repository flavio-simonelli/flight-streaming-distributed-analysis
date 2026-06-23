package it.uniroma2.sae.query.distribution;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import java.io.Serial;

/**
 * Handles partial emissions for Q3 global delay distribution.
 * Fired periodically by Flink's ContinuousEventTimeTrigger.
 */
public class DelayDistributionGlobalWindowProcessor 
        extends ProcessWindowFunction<DelayDistributionAccumulator, DelayDistributionResult, Tuple2<String, Integer>, GlobalWindow> {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final long DATASET_START_MS = 1735689600000L; // 2025-01-01 00:00:00 UTC

    @Override
    public void process(
            Tuple2<String, Integer> key,
            Context context,
            Iterable<DelayDistributionAccumulator> elements,
            Collector<DelayDistributionResult> out) {

        DelayDistributionAccumulator acc = elements.iterator().next();

        long currentWindowEnd = context.currentWatermark();
        if (currentWindowEnd < 0) {
            currentWindowEnd = System.currentTimeMillis();
        }

        out.collect(new DelayDistributionResult(
                DATASET_START_MS,
                currentWindowEnd,
                "global",
                key.f0, // airline
                key.f1, // hour slot
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
