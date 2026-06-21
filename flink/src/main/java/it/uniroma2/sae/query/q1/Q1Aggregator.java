package it.uniroma2.sae.query.q1;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental AggregateFunction for Query 1.
 *
 * Processes each FlightRecord as it arrives, maintaining running counters
 * in a Q1Accumulator. Memory usage is O(1) per keyed partition.
 *
 * Short-circuit pattern: cancelled and diverted flights are counted
 * but excluded from delay statistics.
 */
public class Q1Aggregator implements AggregateFunction<FlightRecord, Q1Accumulator, Q1Accumulator> {

    private static final double LATE_THRESHOLD_MINUTES = 15.0;

    @Override
    public Q1Accumulator createAccumulator() {
        return new Q1Accumulator();
    }

    @Override
    public Q1Accumulator add(FlightRecord event, Q1Accumulator acc) {
        acc.numFlights++;

        // Short-circuit for cancelled flights: count but skip delay stats
        if (event.isCancelled()) {
            acc.cancelled++;
            return acc;
        }

        // Short-circuit for diverted flights: count but skip delay stats
        if (event.isDiverted()) {
            acc.diverted++;
            return acc;
        }

        // Completed flight: accumulate delay metrics
        acc.completed++;
        acc.sumDepDelay += event.getDepDelay();
        acc.countDelay++;

        if (event.getDepDelay() > LATE_THRESHOLD_MINUTES) {
            acc.lateDepartures++;
        }

        return acc;
    }

    /** Pass-through: the ProcessWindowFunction reads the accumulator directly. */
    @Override
    public Q1Accumulator getResult(Q1Accumulator acc) {
        return acc;
    }

    @Override
    public Q1Accumulator merge(Q1Accumulator a, Q1Accumulator b) {
        a.numFlights     += b.numFlights;
        a.cancelled      += b.cancelled;
        a.diverted       += b.diverted;
        a.completed      += b.completed;
        a.lateDepartures += b.lateDepartures;
        a.sumDepDelay    += b.sumDepDelay;
        a.countDelay     += b.countDelay;
        return a;
    }
}
