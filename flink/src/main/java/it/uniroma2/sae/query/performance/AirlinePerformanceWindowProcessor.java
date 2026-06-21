package it.uniroma2.sae.query.performance;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fires at window end to compute rates and average delays, outputting a JSON string.
 */
public class AirlinePerformanceWindowProcessor
        extends ProcessWindowFunction<AirlinePerformanceAccumulator, String, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(AirlinePerformanceWindowProcessor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void process(
            String airline,
            Context ctx,
            Iterable<AirlinePerformanceAccumulator> accumulators,
            Collector<String> out) throws Exception {

        AirlinePerformanceAccumulator acc = accumulators.iterator().next();
        long windowStart = ctx.window().getStart();
        long windowEnd   = ctx.window().getEnd();

        double depDelayMean      = acc.countDelay > 0 ? acc.sumDepDelay / acc.countDelay : 0.0;
        double cancellationRate  = acc.numFlights > 0 ? (double) acc.cancelled / acc.numFlights * 100.0 : 0.0;
        double lateDepartureRate = acc.numFlights > 0 ? (double) acc.lateDepartures / acc.numFlights * 100.0 : 0.0;

        AirlinePerformanceOutput result = new AirlinePerformanceOutput(
                windowStart, windowEnd, airline,
                acc.numFlights, acc.cancelled, acc.diverted, acc.completed,
                depDelayMean,
                cancellationRate, lateDepartureRate);

        LOG.debug("AirlinePerformance window closed: {}", result);
        out.collect(MAPPER.writeValueAsString(result));
    }
}
