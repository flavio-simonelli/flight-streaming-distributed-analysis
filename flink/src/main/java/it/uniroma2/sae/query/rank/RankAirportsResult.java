package it.uniroma2.sae.query.rank;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Result data model holding metrics for a specific airport.
 * Serialized as JSON with explicit window type identification and ranking.
 */
public class RankAirportsResult implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @JsonProperty("window_start")
    private long windowStart;

    @JsonProperty("window_end")
    private long windowEnd;

    @JsonProperty("window_type")
    private String windowType; // "1h", "6h", "global"

    @JsonProperty("rank")
    private int rank;

    @JsonProperty("origin_airport_id")
    private int originAirportId;

    @JsonProperty("num_flights")
    private int numFlights;

    @JsonProperty("severe_delays")
    private int severeDelays;

    @JsonProperty("dep_delay_mean")
    private double depDelayMean;

    @JsonProperty("dep_delay_max")
    private double depDelayMax;

    @JsonProperty("delayed_flights")
    private List<RankAirportsDelayedFlight> delayedFlights;

    @JsonIgnore
    private long maxSystemIngestionTime;

    @JsonIgnore
    public long getMaxSystemIngestionTime() {
        return maxSystemIngestionTime;
    }

    @JsonIgnore
    public void setMaxSystemIngestionTime(long maxSystemIngestionTime) {
        this.maxSystemIngestionTime = maxSystemIngestionTime;
    }

    public RankAirportsResult() {}

    public RankAirportsResult(
            long windowStart, long windowEnd, String windowType,
            int originAirportId, int numFlights, int severeDelays,
            double depDelayMean, double depDelayMax,
            List<RankAirportsDelayedFlight> delayedFlights) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.windowType = windowType;
        this.originAirportId = originAirportId;
        this.numFlights = numFlights;
        this.severeDelays = severeDelays;
        this.depDelayMean = depDelayMean;
        this.depDelayMax = depDelayMax;
        this.delayedFlights = delayedFlights;
    }

    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public String getWindowType() { return windowType; }
    public void setWindowType(String windowType) { this.windowType = windowType; }

    public int getRank() { return rank; }
    public void setRank(int rank) { this.rank = rank; }

    public int getOriginAirportId() { return originAirportId; }
    public void setOriginAirportId(int originAirportId) { this.originAirportId = originAirportId; }

    public int getNumFlights() { return numFlights; }
    public void setNumFlights(int numFlights) { this.numFlights = numFlights; }

    public int getSevereDelays() { return severeDelays; }
    public void setSevereDelays(int severeDelays) { this.severeDelays = severeDelays; }

    public double getDepDelayMean() { return depDelayMean; }
    public void setDepDelayMean(double depDelayMean) { this.depDelayMean = depDelayMean; }

    public double getDepDelayMax() { return depDelayMax; }
    public void setDepDelayMax(double depDelayMax) { this.depDelayMax = depDelayMax; }

    public List<RankAirportsDelayedFlight> getDelayedFlights() { return delayedFlights; }
    public void setDelayedFlights(List<RankAirportsDelayedFlight> delayedFlights) { this.delayedFlights = delayedFlights; }
}
