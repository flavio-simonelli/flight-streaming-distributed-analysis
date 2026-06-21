package it.uniroma2.sae.query.q2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serial;

/**
 * Evaluates context statistics at structural event-time window boundaries.
 * Wraps cumulative localized results into complete downstream result models.
 */
public class Q2WindowProcessor extends ProcessWindowFunction<Q2Accumulator, Q2AirportResult, Integer, TimeWindow> {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public void process(
            Integer key,
            Context context,
            Iterable<Q2Accumulator> elements,
            Collector<Q2AirportResult> out) {

        Q2Accumulator acc = elements.iterator().next();
        long start = context.window().getStart();
        long end = context.window().getEnd();

        double mean = acc.getNumFlights() > 0 ? acc.getSumDepDelay() / acc.getNumFlights() : 0.0;

        out.collect(new Q2AirportResult(
                start,
                end,
                key,
                acc.getNumFlights(),
                acc.getSevereDelays(),
                mean,
                acc.getMaxDepDelay(),
                acc.getDelayedFlights()
        ));
    }
}