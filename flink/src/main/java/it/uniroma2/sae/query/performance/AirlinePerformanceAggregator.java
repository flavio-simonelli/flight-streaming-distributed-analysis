package it.uniroma2.sae.query.performance;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental AggregateFunction for AirlinePerformanceQuery.
 * Processes each flight event and updates running counters.
 */
public class AirlinePerformanceAggregator implements AggregateFunction<AirlinePerformanceEvent, AirlinePerformanceAccumulator, AirlinePerformanceAccumulator> {

    /**
     * Initializes a new, empty accumulator for a window instance.
     *
     * @return a fresh AirlinePerformanceAccumulator instance with zeroed metrics
     */
    @Override
    public AirlinePerformanceAccumulator createAccumulator() {
        return new AirlinePerformanceAccumulator();
    }

    /**
     * Incrementally updates the accumulator with the metrics of an incoming flight record.
     * Splits logic between canceled and operated flights based on business requirements.
     *
     * @param event the incoming flight record to process
     * @param acc the current running window accumulator
     * @return the updated accumulator state container
     */
    @Override
    public AirlinePerformanceAccumulator add(AirlinePerformanceEvent event, AirlinePerformanceAccumulator acc) {
        // If the flight was canceled, handle it separately without tracking delay telemetry
        if (event.isCancelled()) {
            acc.addCancelledFlight();
        } else {
            // For operated flights, process delays, late thresholds, and diversion/completion state
            acc.addOperatedFlight(
                event.getDepDelay(),
                event.isDiverted()
            );
        }

        long t = event.getSystemIngestionTime();
        if (t > 0) {
            acc.maxSystemIngestionTime = Math.max(acc.maxSystemIngestionTime, t);
            acc.minSystemIngestionTime = Math.min(acc.minSystemIngestionTime, t);
            acc.sumSystemIngestionTime += t;
            acc.systemIngestionTimeCount++;
        }
        return acc;
    }

    /**
     * Extracts the final raw aggregation results from the accumulated state.
     *
     * @param acc the final window accumulator containing calculated state
     * @return the exact same accumulator instance containing final values
     */
    @Override
    public AirlinePerformanceAccumulator getResult(AirlinePerformanceAccumulator acc) {
        return acc;
    }

    /**
     * Merges two partial window accumulators into a single unified state container.
     *
     * @param a the primary accumulator that absorbs data
     * @param b the secondary accumulator providing the delta metrics
     * @return the primary accumulator 'a' updated with combined statistics
     */
    @Override
    public AirlinePerformanceAccumulator merge(AirlinePerformanceAccumulator a, AirlinePerformanceAccumulator b) {
        a.mergeWith(b);
        return a;
    }
}