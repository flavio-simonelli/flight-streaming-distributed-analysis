package it.uniroma2.sae.query.components;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.FilterFunction;

import java.io.Serial;
import java.util.Set;

/**
 * Dedicated FilterFunction to isolate targeted airline carriers.
 * This component is shared across query topologies to enforce consistent carrier filtering.
 */
public class TargetAirlineFilter implements FilterFunction<FlightRecord> {

    @Serial
    private static final long serialVersionUID = 1L;

    /** The immutable set of specific airline carrier codes targeted by the query. */
    private final Set<String> targetAirlines;

    /**
     * Constructs a new filter function parameterized with the allowed airline carriers.
     *
     * @param targetAirlines the set of airline unique codes to retain in the pipeline
     */
    public TargetAirlineFilter(Set<String> targetAirlines) {
        this.targetAirlines = targetAirlines;
    }

    /**
     * Evaluates if the flight record belongs to one of the targeted airlines.
     *
     * @param event the flight event to test
     * @return true if the flight is not null and matches a target airline code, false otherwise
     * @throws Exception if an error occurs during filtering execution
     */
    @Override
    public boolean filter(FlightRecord event) throws Exception {
        return event != null && targetAirlines.contains(event.getAirline());
    }
}