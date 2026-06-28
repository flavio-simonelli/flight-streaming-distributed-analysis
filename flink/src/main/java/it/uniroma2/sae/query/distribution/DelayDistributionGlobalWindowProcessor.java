package it.uniroma2.sae.query.distribution;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.Serial;
import java.util.Iterator;

/**
 * Process-window function used for the unbounded global delay-distribution window.
 * <p>
 * The global window never closes naturally. It is fired periodically by the configured
 * {@link org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger}, which allows
 * the query to publish progressive global percentiles while the stream keeps advancing in event time.
 * </p>
 * <p>
 * The emitted end timestamp is derived from the current watermark rather than processing time, keeping
 * the output deterministic with respect to event-time progress and checkpoint recovery.
 * </p>
 */
public class DelayDistributionGlobalWindowProcessor
        extends ProcessWindowFunction<DelayDistributionAccumulator, DelayDistributionResult, Tuple2<String, Integer>, GlobalWindow> {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Fixed lower bound used to label the global window range for the current simulation dataset. */
    private static final long DATASET_START_MS = 1735689600000L; // 2025-01-01 00:00:00 UTC

    /**
     * Emits the current global distribution snapshot for one airline/hour key.
     * <p>
     * If no watermark has been observed yet, the method skips the emission because the event-time
     * upper bound of the global result would not be meaningful.
     * </p>
     *
     * @param key the composite key containing airline code and scheduled departure hour
     * @param context the global window context exposing the current watermark
     * @param elements the iterable containing the current global accumulator snapshot
     * @param out the collector receiving the progressive global result
     */
    @Override
    public void process(
            Tuple2<String, Integer> key,
            Context context,
            Iterable<DelayDistributionAccumulator> elements,
            Collector<DelayDistributionResult> out) {

        long currentWindowEnd = context.currentWatermark();
        if (currentWindowEnd == Long.MIN_VALUE) {
            return;
        }

        Iterator<DelayDistributionAccumulator> iterator = elements.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        DelayDistributionAccumulator acc = iterator.next();

        out.collect(DelayDistributionResult.fromAccumulator(
                DATASET_START_MS,
                currentWindowEnd,
                "global",
                key,
                acc
        ));
    }
}