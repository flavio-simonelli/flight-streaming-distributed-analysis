package it.uniroma2.sae.query.q1;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessWindowFunction for Query 1.
 *
 * Fires once per (airline, window) pair when the watermark closes the window.
 * Reads the pre-aggregated Q1Accumulator from the AggregateFunction and
 * computes the final derived metrics (means and rates).
 *
 * Serializes the result to JSON and emits a String to the Kafka sink.
 */
public class Q1WindowProcessor
        extends ProcessWindowFunction<Q1Accumulator, String, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(Q1WindowProcessor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void process(
            String airline,
            Context ctx,
            Iterable<Q1Accumulator> accumulators,
            Collector<String> out) throws Exception {

        Q1Accumulator acc = accumulators.iterator().next();
        long windowStart = ctx.window().getStart();
        long windowEnd   = ctx.window().getEnd();

        // Compute derived metrics; guard against division by zero
        double depDelayMean      = acc.countDelay > 0 ? acc.sumDepDelay / acc.countDelay : 0.0;
        double arrDelayMean      = acc.countDelay > 0 ? acc.sumArrDelay / acc.countDelay : 0.0;
        double cancellationRate  = acc.numFlights > 0 ? (double) acc.cancelled     / acc.numFlights * 100.0 : 0.0;
        double lateDepartureRate = acc.numFlights > 0 ? (double) acc.lateDepartures / acc.numFlights * 100.0 : 0.0;

        Q1OutputRecord result = new Q1OutputRecord(
                windowStart, windowEnd, airline,
                acc.numFlights, acc.cancelled, acc.diverted, acc.completed,
                depDelayMean, arrDelayMean,
                cancellationRate, lateDepartureRate);

        LOG.info("Q1 window closed: {}", result);
        out.collect(MAPPER.writeValueAsString(result));
    }
}
