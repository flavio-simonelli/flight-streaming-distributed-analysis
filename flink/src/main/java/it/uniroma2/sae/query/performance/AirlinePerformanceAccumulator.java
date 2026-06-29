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

    /** The logic threshold defining an official flight departure delay. */
    private static final double LATE_THRESHOLD_MINUTES = 15.0;

    /** The running total of all flights ingested within the window. */
    public long numFlights     = 0;

    /** The aggregate counter tracking canceled flights. */
    public long cancelled      = 0;

    /** The aggregate counter tracking diverted flights. */
    public long diverted       = 0;

    /** The aggregate counter tracking successfully completed flights. */
    public long completed      = 0;

    /** The aggregate counter tracking flights departing with a delay greater than 15 minutes. */
    public long lateDepartures = 0;

    /** The running cumulative sum of all departure delay minutes for average computation. */
    public double sumDepDelay  = 0.0;

    /** The aggregate counter of flights that contributed to the delay metrics sum. */
    public long   countDelay   = 0;

    public long maxSystemIngestionTime = 0L;
    public long minSystemIngestionTime = Long.MAX_VALUE;
    public long sumSystemIngestionTime = 0L;
    public long systemIngestionTimeCount = 0L;

    /**
     * Adds a canceled flight.
     *
     * Canceled flights are counted in the total number of flights,
     * but they are not used to compute departure delay statistics.
     */
    public void addCancelledFlight() {
        numFlights++; // Increment the overarching flight count
        cancelled++;  // Mark this explicitly as a cancellation occurrence
    }

    /**
     * Adds a non-canceled flight.
     *
     * Non-canceled flights contribute to departure delay statistics.
     * If the flight is diverted, it is counted as diverted;
     * otherwise it is counted as completed.
     *
     * @param depDelay  The departure delay of the flight.
     * @param isDiverted  Whether the flight was diverted.
     */
    public void addOperatedFlight(double depDelay, boolean isDiverted) {
        numFlights++; // Every processed event increments the grand total tracking size

        sumDepDelay += depDelay; // Track cumulative minutes for downstream average evaluations
        countDelay++;            // Track sample size for calculating the exact average delay later

        // Evaluate if the flight departure exceeds the formal airline delay threshold boundaries
        if (depDelay > LATE_THRESHOLD_MINUTES) {
            lateDepartures++;
        }

        // Branch execution state based on whether the flight reached its destination or was rerouted
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
        // Aggregate primitive metric counters together to form the unified result view
        this.numFlights     += other.numFlights;
        this.cancelled      += other.cancelled;
        this.diverted       += other.diverted;
        this.completed      += other.completed;
        this.lateDepartures += other.lateDepartures;
        this.sumDepDelay    += other.sumDepDelay;
        this.countDelay     += other.countDelay;
        this.maxSystemIngestionTime = Math.max(this.maxSystemIngestionTime, other.maxSystemIngestionTime);
        if (other.minSystemIngestionTime != Long.MAX_VALUE) {
            this.minSystemIngestionTime = Math.min(this.minSystemIngestionTime, other.minSystemIngestionTime);
        }
        this.sumSystemIngestionTime += other.sumSystemIngestionTime;
        this.systemIngestionTimeCount += other.systemIngestionTimeCount;
    }

    /**
     * Returns the number of flights that were not canceled.
     *
     * @return flights not canceled
     */
    public long getNonCanceledFlights() {
        return numFlights - cancelled;
    }
}