package it.uniroma2.sae.query.distribution;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregator for DelayDistributionQuery.
 * Filters out cancelled and diverted flights, adding completed flight delays to the sketch accumulator.
 */
public class DelayDistributionAggregator 
        implements AggregateFunction<FlightRecord, DelayDistributionAccumulator, DelayDistributionAccumulator> {

    @Override
    public DelayDistributionAccumulator createAccumulator() {
        return new DelayDistributionAccumulator();
    }

    @Override
    public DelayDistributionAccumulator add(FlightRecord value, DelayDistributionAccumulator accumulator) {
        accumulator.add(value.getDepDelay());
        return accumulator;
    }

    @Override
    public DelayDistributionAccumulator getResult(DelayDistributionAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public DelayDistributionAccumulator merge(DelayDistributionAccumulator a, DelayDistributionAccumulator b) {
        a.merge(b);
        return a;
    }
}
