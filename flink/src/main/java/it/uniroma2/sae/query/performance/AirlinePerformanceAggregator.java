package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental AggregateFunction for AirlinePerformanceQuery.
 * Processes each flight event and updates running counters.
 */
public class AirlinePerformanceAggregator implements AggregateFunction<FlightRecord, AirlinePerformanceAccumulator, AirlinePerformanceAccumulator> {

    @Override
    public AirlinePerformanceAccumulator createAccumulator() {
        return new AirlinePerformanceAccumulator();
    }

    @Override
    public AirlinePerformanceAccumulator add( FlightRecord event, AirlinePerformanceAccumulator acc) {

        if (event.isCancelled()) {
            acc.addCancelledFlight();
            return acc;
        }

        acc.addOperatedFlight(
            event.getDepDelay(),
            event.isDiverted()
        );

        return acc;
    }

    @Override
    public AirlinePerformanceAccumulator getResult(AirlinePerformanceAccumulator acc) {
        return acc;
    }

    @Override
    public AirlinePerformanceAccumulator merge(AirlinePerformanceAccumulator a, AirlinePerformanceAccumulator b) {
        a.mergeWith(b);
        return a;
    }
}