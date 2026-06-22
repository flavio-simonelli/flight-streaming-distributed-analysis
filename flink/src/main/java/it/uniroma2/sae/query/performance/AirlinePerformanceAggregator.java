package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental AggregateFunction for AirlinePerformanceQuery.
 * Processes each flight event and updates running counters.
 */
public class AirlinePerformanceAggregator 
        implements AggregateFunction<FlightRecord, AirlinePerformanceAccumulator, AirlinePerformanceAccumulator> {

    private static final double LATE_THRESHOLD_MINUTES = 15.0;

    @Override
    public AirlinePerformanceAccumulator createAccumulator() {
        return new AirlinePerformanceAccumulator();
    }

    @Override
    public AirlinePerformanceAccumulator add(FlightRecord event, AirlinePerformanceAccumulator acc) {
        acc.numFlights++;

        if (event.isCancelled()) {
            acc.cancelled++;
            return acc;
        }

        acc.sumDepDelay += event.getDepDelay();
        acc.countDelay++;

        if (event.getDepDelay() > LATE_THRESHOLD_MINUTES) {
            acc.lateDepartures++;
        }

        if (event.isDiverted()) {
            acc.diverted++;
        } else {
            acc.completed++;
        }

        return acc;
    }

    @Override
    public AirlinePerformanceAccumulator getResult(AirlinePerformanceAccumulator acc) {
        return acc;
    }

    @Override
    public AirlinePerformanceAccumulator merge(AirlinePerformanceAccumulator a, AirlinePerformanceAccumulator b) {
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
