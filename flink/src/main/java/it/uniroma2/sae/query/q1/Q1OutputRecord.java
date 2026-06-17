package it.uniroma2.sae.query.q1;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Output record produced by Query 1 for each airline per tumbling window.
 * Serialized as JSON and written to the Kafka output topic.
 */
public class Q1OutputRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("window_start")
    private final long windowStart;   // epoch millis

    @JsonProperty("window_end")
    private final long windowEnd;     // epoch millis

    @JsonProperty("airline")
    private final String airline;

    @JsonProperty("num_flights")
    private final long numFlights;

    @JsonProperty("cancelled")
    private final long cancelled;

    @JsonProperty("diverted")
    private final long diverted;

    @JsonProperty("completed")
    private final long completed;

    @JsonProperty("dep_delay_mean")
    private final double depDelayMean;

    @JsonProperty("arr_delay_mean")
    private final double arrDelayMean;

    @JsonProperty("cancellation_rate")
    private final double cancellationRate;

    @JsonProperty("late_departure_rate")
    private final double lateDepartureRate;

    public Q1OutputRecord(
            long windowStart, long windowEnd, String airline,
            long numFlights, long cancelled, long diverted, long completed,
            double depDelayMean, double arrDelayMean,
            double cancellationRate, double lateDepartureRate) {
        this.windowStart       = windowStart;
        this.windowEnd         = windowEnd;
        this.airline           = airline;
        this.numFlights        = numFlights;
        this.cancelled         = cancelled;
        this.diverted          = diverted;
        this.completed         = completed;
        this.depDelayMean      = depDelayMean;
        this.arrDelayMean      = arrDelayMean;
        this.cancellationRate  = cancellationRate;
        this.lateDepartureRate = lateDepartureRate;
    }

    public long getWindowStart()      { return windowStart; }
    public long getWindowEnd()        { return windowEnd; }
    public String getAirline()        { return airline; }
    public long getNumFlights()       { return numFlights; }
    public long getCancelled()        { return cancelled; }
    public long getDiverted()         { return diverted; }
    public long getCompleted()        { return completed; }
    public double getDepDelayMean()   { return depDelayMean; }
    public double getArrDelayMean()   { return arrDelayMean; }
    public double getCancellationRate()  { return cancellationRate; }
    public double getLateDepartureRate() { return lateDepartureRate; }

    @Override
    public String toString() {
        return "Q1OutputRecord{" +
                "airline='" + airline + '\'' +
                ", window=[" + windowStart + "," + windowEnd + "]" +
                ", numFlights=" + numFlights +
                ", cancelled=" + cancelled +
                ", diverted=" + diverted +
                ", depDelayMean=" + depDelayMean +
                ", cancellationRate=" + cancellationRate +
                ", lateDepartureRate=" + lateDepartureRate +
                '}';
    }
}
