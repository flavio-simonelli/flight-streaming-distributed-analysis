package it.uniroma2.sae.query.q2;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

public class Q2Aggregator implements AggregateFunction<FlightRecord, Q2Accumulator, Q2Accumulator> {
    private static final long serialVersionUID = 1L;

    @Override
    public Q2Accumulator createAccumulator() {
        return new Q2Accumulator();
    }

    @Override
    public Q2Accumulator add(FlightRecord value, Q2Accumulator accumulator) {
        // Exclude cancelled and diverted flights
        if (value.isCancelled() || value.isDiverted()) {
            return accumulator;
        }
        accumulator.add(value.getAirline(), value.getDest(), value.getDepDelay());
        return accumulator;
    }

    @Override
    public Q2Accumulator getResult(Q2Accumulator accumulator) {
        return accumulator;
    }

    @Override
    public Q2Accumulator merge(Q2Accumulator a, Q2Accumulator b) {
        return a.merge(b);
    }
}
