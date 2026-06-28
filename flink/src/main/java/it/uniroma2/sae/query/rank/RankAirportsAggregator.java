package it.uniroma2.sae.query.rank;

import org.apache.flink.api.common.functions.AggregateFunction;
import java.io.Serial;

/**
 * Aggregator for RankAirportsQuery.
 * Adds lightweight flight events to the accumulator.
 */
public class RankAirportsAggregator 
        implements AggregateFunction<RankAirportsEvent, RankAirportsAccumulator, RankAirportsAccumulator> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public RankAirportsAccumulator createAccumulator() {
        return new RankAirportsAccumulator();
    }

    @Override
    public RankAirportsAccumulator add(RankAirportsEvent value, RankAirportsAccumulator accumulator) {
        accumulator.add(value.getAirline(), String.valueOf(value.getDestinationAirportId()), value.getDepDelay());
        return accumulator;
    }

    @Override
    public RankAirportsAccumulator getResult(RankAirportsAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public RankAirportsAccumulator merge(RankAirportsAccumulator a, RankAirportsAccumulator b) {
        return a.merge(b);
    }
}
