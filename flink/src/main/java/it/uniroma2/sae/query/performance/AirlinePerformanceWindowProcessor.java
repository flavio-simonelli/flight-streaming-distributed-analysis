package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.utils.DateUtils;
import it.uniroma2.sae.utils.MathUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fires at window end to compute rates and average delays, outputting a JSON string.
 */
public class AirlinePerformanceWindowProcessor
        extends ProcessWindowFunction<AirlinePerformanceAccumulator, AirlinePerformanceResult, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(AirlinePerformanceWindowProcessor.class);

    @Override
    public void process(
            String airline,
            Context ctx,
            Iterable<AirlinePerformanceAccumulator> accumulators,
            Collector<AirlinePerformanceResult> out) throws Exception {

        AirlinePerformanceAccumulator acc = accumulators.iterator().next();
        double depDelayMean      = MathUtils.safeDivideRounded(acc.sumDepDelay, acc.countDelay);
        double cancellationRate  = MathUtils.safeDividePercent(acc.cancelled, acc.numFlights);

        long nonCancelledFlights = acc.numFlights - acc.cancelled;
        double lateDepartureRate = MathUtils.safeDividePercent(acc.lateDepartures, nonCancelledFlights);

        long windowStartRaw = ctx.window().getStart();
        long windowEndRaw   = ctx.window().getEnd();

        String windowStartStr = DateUtils.formatTimestamp(windowStartRaw);
        String windowEndStr   = DateUtils.formatTimestamp(windowEndRaw);

        AirlinePerformanceResult result = new AirlinePerformanceResult(
                windowStartStr, windowEndStr, airline,
                acc.numFlights, acc.cancelled, acc.diverted, acc.completed,
                depDelayMean,
                cancellationRate, lateDepartureRate);

        LOG.debug("AirlinePerformance window closed: {}", result);
        out.collect(result);
    }
}
