package it.uniroma2.sae.query.components;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.FilterFunction;

import java.io.Serial;

/**
 * Dedicated FilterFunction to isolate active flights by discarding operational anomalies.
 * This component filters out flights that have been either cancelled or diverted,
 * ensuring downstream analytics process exclusively completed flight events.
 */
public class ActiveFlightFilter implements FilterFunction<FlightRecord> {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Evaluates if the flight record represents a completed operational flight.
     * Discards records that are null, cancelled, or diverted.
     *
     * @param event the flight event to test
     * @return true if the flight is active (not null, not cancelled, and not diverted), false otherwise
     */
    @Override
    public boolean filter(FlightRecord event) {
        return event != null && !event.isCancelled() && !event.isDiverted();
    }
}