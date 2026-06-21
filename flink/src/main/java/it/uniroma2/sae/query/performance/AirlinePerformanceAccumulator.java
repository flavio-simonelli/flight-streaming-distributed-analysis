package it.uniroma2.sae.query.performance;

import java.io.Serial;
import java.io.Serializable;

/**
 * Mutable accumulator for incremental aggregation of airline performance.
 * O(1) memory footprint.
 */
public class AirlinePerformanceAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public long numFlights     = 0;
    public long cancelled      = 0;
    public long diverted       = 0;
    public long completed      = 0;
    public long lateDepartures = 0;

    public double sumDepDelay  = 0.0;
    public long   countDelay   = 0;
}
