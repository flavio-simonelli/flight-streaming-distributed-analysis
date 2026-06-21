package it.uniroma2.sae.query.common;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import java.io.Serial;
import java.io.Serializable;
import java.util.Set;

/**
 * Base abstract class for airline-specific queries (Query 1 and Query 3).
 * Shares common targeting rules and operators for cluster execution stability.
 */
public abstract class BaseAirlineQuery implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public static final Set<String> TARGET_AIRLINES = Set.of("AA", "DL", "UA", "WN");

    /**
     * Dedicated FilterFunction to isolate target carriers.
     */
    public static class TargetAirlineFilter implements FilterFunction<FlightRecord> {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(FlightRecord event) throws Exception {
            return event != null && TARGET_AIRLINES.contains(event.getAirline());
        }
    }
}
