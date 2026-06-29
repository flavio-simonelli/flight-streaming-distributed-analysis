package it.uniroma2.sae.query.distribution;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregator for DelayDistributionQuery.
 * Adds lightweight flight delay events to the sketch accumulator.
 */
public class DelayDistributionAggregator 
        implements AggregateFunction<DelayDistributionEvent, DelayDistributionAccumulator, DelayDistributionAccumulator> {

    @Override
    public DelayDistributionAccumulator createAccumulator() {
        return new DelayDistributionAccumulator();
    }

    @Override
    public DelayDistributionAccumulator add(DelayDistributionEvent value, DelayDistributionAccumulator accumulator) {
        accumulator.add(value.getDepDelay());
        accumulator.maxSystemIngestionTime = Math.max(accumulator.maxSystemIngestionTime, value.getSystemIngestionTime());
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
