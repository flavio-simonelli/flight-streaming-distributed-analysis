package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.metrics.ProcessingLatencyTracker;
import it.uniroma2.sae.utils.MathUtils;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Fires at window end to compute rates and average delays.
 * Evaluates the final business indicators (averages and percentages) required by Query 1.
 */
public class AirlinePerformanceWindowProcessor extends ProcessWindowFunction<AirlinePerformanceAccumulator, AirlinePerformanceResult, String, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(AirlinePerformanceWindowProcessor.class);
    private transient ProcessingLatencyTracker latencyTracker;

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        this.latencyTracker = new ProcessingLatencyTracker("q1_window");
        this.latencyTracker.register(getRuntimeContext().getMetricGroup());
    }

    /**
     * Processes the accumulated window data to finalize operational metrics for a specific airline.
     * Evaluates averages, cancellation rates, and late departure rates before emitting results.
     *
     * @param airline the airline key that identifies the current data stream partition
     * @param ctx the context containing metadata about the current window execution bounds
     * @param accumulators the iterable collection containing the incremental window state accumulator
     * @param out the collector utilized to emit the structured evaluation result downstream
     * @throws Exception if any processing or formatting dependency fails
     */
    @Override
    public void process(
            String airline,
            Context ctx,
            Iterable<AirlinePerformanceAccumulator> accumulators,
            Collector<AirlinePerformanceResult> out) throws Exception {

        long start = System.nanoTime();
        try {
            processInternal(airline, ctx, accumulators, out);
        } finally {
            double duration = (System.nanoTime() - start) / 1_000_000.0;
            if (latencyTracker != null) {
                latencyTracker.updateOperator(duration);
                // Extract accumulator to get maxSystemIngestionTime
                Iterator<AirlinePerformanceAccumulator> iterator = accumulators.iterator();
                if (iterator.hasNext()) {
                    AirlinePerformanceAccumulator acc = iterator.next();
                    long avgIngest = acc.systemIngestionTimeCount > 0 ? (acc.sumSystemIngestionTime / acc.systemIngestionTimeCount) : 0L;
                    latencyTracker.updateE2E(acc.maxSystemIngestionTime, acc.minSystemIngestionTime, avgIngest);
                }
            }
        }
    }

    private void processInternal(
            String airline,
            Context ctx,
            Iterable<AirlinePerformanceAccumulator> accumulators,
            Collector<AirlinePerformanceResult> out) throws Exception {

        // Instantiate an iterator over the window's accumulated state
        Iterator<AirlinePerformanceAccumulator> iterator = accumulators.iterator();

        // Extract raw epoch millisecond boundaries for the current time window instance
        long windowStart = ctx.window().getStart();
        long windowEnd   = ctx.window().getEnd();

        // Fallback safety check: handle the rare scenario where an empty accumulator is fired
        if (!iterator.hasNext()) {
            LOG.warn("No accumulator found for airline {}", airline);

            // Construct an empty tracking result to ensure structural continuity in downstream systems
            AirlinePerformanceResult result = new AirlinePerformanceResult(
                    windowStart,
                    windowEnd,
                    airline,
                    0,
                    0,
                    0,
                    0,
                    0.0,
                    0.0,
                    0.0
            );

            out.collect(result);
            return;
        }

        // Retrieve the single, incrementally filled accumulator containing the raw flight metrics
        AirlinePerformanceAccumulator acc = iterator.next();

        // Requirement Q1: Calculate the mathematical mean of departure delays for operated flights
        double depDelayMean = MathUtils.safeDivideRounded(
            acc.sumDepDelay,
            acc.countDelay
        );

        // Requirement Q1: Calculate the cancellation percentage rate relative to total observed flights
        double cancellationRate = MathUtils.safeDividePercent(
            acc.cancelled,
            acc.numFlights
        );

        // Requirement Q1: Calculate late departure percentage rate relative to non-canceled flights
        double lateDepartureRate = MathUtils.safeDividePercent(
            acc.lateDepartures,
            acc.getNonCanceledFlights()
        );

        // Map the finalized calculations and counter dimensions into the targeted result structure
        AirlinePerformanceResult result = new AirlinePerformanceResult(
            windowStart,
            windowEnd,
            airline,
            acc.numFlights,
            acc.cancelled,
            acc.diverted,
            acc.completed,
            depDelayMean,
            cancellationRate,
            lateDepartureRate
        );

        // Log the processed record context for runtime query observability and verification
        LOG.debug("AirlinePerformance window closed: {}", result);

        // Push the fully compiled operational metrics snapshot to the configured Kafka output sink
        out.collect(result);
    }
}