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

    private static final double LATE_THRESHOLD_MINUTES = 15.0;

    public long numFlights     = 0;
    public long cancelled      = 0;
    public long diverted       = 0;
    public long completed      = 0;
    public long lateDepartures = 0;

    public double sumDepDelay  = 0.0;
    public long   countDelay   = 0;

    /**
     * Adds a cancelled flight.
     *
     * Cancelled flights are counted in the total number of flights,
     * but they are not used to compute departure delay statistics.
     */
    public void addCancelledFlight() {
        numFlights++;
        cancelled++;
    }

    /**
     * Adds a non-cancelled flight.
     *
     * Non-cancelled flights contribute to departure delay statistics.
     * If the flight is diverted, it is counted as diverted;
     * otherwise it is counted as completed.
     *
     * @param depDelay  The departure delay of the flight.
     * @param isDiverted  Whether the flight was diverted.
     */
    public void addOperatedFlight(double depDelay, boolean isDiverted) {
        numFlights++;

        sumDepDelay += depDelay;
        countDelay++;

        if (depDelay > LATE_THRESHOLD_MINUTES) {
            lateDepartures++;
        }

        if (isDiverted) {
            diverted++;
        } else {
            completed++;
        }
    }

    /**
     * Merges another accumulator into this one.
     *
     * @param other  The other accumulator to merge with this one.
     */
    public void mergeWith(AirlinePerformanceAccumulator other) {
        this.numFlights     += other.numFlights;
        this.cancelled      += other.cancelled;
        this.diverted       += other.diverted;
        this.completed      += other.completed;
        this.lateDepartures += other.lateDepartures;
        this.sumDepDelay    += other.sumDepDelay;
        this.countDelay     += other.countDelay;
    }

    /**
     * Returns the number of flights that were not cancelled.
     */
    public long getNonCancelledFlights() {
        return numFlights - cancelled;
    }
}