package it.uniroma2.sae.query.rank;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import java.io.Serial;

/**
 * Aggregator for RankAirportsQuery.
 * Filters out cancelled/diverted flights and adds delays to the accumulator.
 */
public class RankAirportsAggregator 
        implements AggregateFunction<FlightRecord, RankAirportsAccumulator, RankAirportsAccumulator> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public RankAirportsAccumulator createAccumulator() {
        return new RankAirportsAccumulator();
    }

    @Override
    public RankAirportsAccumulator add(FlightRecord value, RankAirportsAccumulator accumulator) {
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
