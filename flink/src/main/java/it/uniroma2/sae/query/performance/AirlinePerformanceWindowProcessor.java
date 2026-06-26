package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.utils.DateUtils;
import it.uniroma2.sae.utils.MathUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Fires at window end to compute rates and average delays.
 */
public class AirlinePerformanceWindowProcessor extends ProcessWindowFunction<AirlinePerformanceAccumulator, AirlinePerformanceResult, String, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(AirlinePerformanceWindowProcessor.class);

    @Override
    public void process(
            String airline,
            Context ctx,
            Iterable<AirlinePerformanceAccumulator> accumulators,
            Collector<AirlinePerformanceResult> out) throws Exception {

        Iterator<AirlinePerformanceAccumulator> iterator = accumulators.iterator();

        long windowStartRaw = ctx.window().getStart();
        long windowEndRaw   = ctx.window().getEnd();

        String windowStartStr = DateUtils.formatTimestamp(windowStartRaw);
        String windowEndStr   = DateUtils.formatTimestamp(windowEndRaw);

        if (!iterator.hasNext()) {
            LOG.warn("No accumulator found for airline {}", airline);

            AirlinePerformanceResult result = new AirlinePerformanceResult(
                windowStartStr,
                windowEndStr,
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

        AirlinePerformanceAccumulator acc = iterator.next();

        double depDelayMean = MathUtils.safeDivideRounded(
            acc.sumDepDelay,
            acc.countDelay
        );

        double cancellationRate = MathUtils.safeDividePercent(
            acc.cancelled,
            acc.numFlights
        );

        double lateDepartureRate = MathUtils.safeDividePercent(
            acc.lateDepartures,
            acc.getNonCancelledFlights()
        );

        AirlinePerformanceResult result = new AirlinePerformanceResult(
            windowStartStr,
            windowEndStr,
            airline,
            acc.numFlights,
            acc.cancelled,
            acc.diverted,
            acc.completed,
            depDelayMean,
            cancellationRate,
            lateDepartureRate
        );

        LOG.debug("AirlinePerformance window closed: {}", result);
        out.collect(result);
    }
}