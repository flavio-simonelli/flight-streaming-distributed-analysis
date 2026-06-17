package it.uniroma2.sae.query.q1;

import java.io.Serializable;

/**
 * Mutable accumulator for the incremental aggregation in Query 1.
 * Holds per-airline running totals updated record by record.
 * O(1) memory footprint regardless of window size.
 */
public class Q1Accumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    public long numFlights     = 0;
    public long cancelled      = 0;
    public long diverted       = 0;
    public long completed      = 0;
    public long lateDepartures = 0;

    // Sums used to compute means at window close
    public double sumDepDelay  = 0.0;
    public double sumArrDelay  = 0.0;
    public long   countDelay   = 0;    // count of completed flights with valid delay data
}
