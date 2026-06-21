package it.uniroma2.sae.query.q2;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Unified messaging POJO holding evaluation results for a distinct airport entity.
 * Suitable for internal streaming exchanges and subsequent transformation jobs.
 */
public class Q2AirportResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private long windowStart;
    private long windowEnd;
    private int originAirportId;
    private int numFlights;
    private int severeDelays;
    private double depDelayMean;
    private double depDelayMax;
    private List<Q2DelayedFlight> delayedFlights;

    public Q2AirportResult() {}

    public Q2AirportResult(long windowStart, long windowEnd, int originAirportId, int numFlights, int severeDelays, double depDelayMean, double depDelayMax, List<Q2DelayedFlight> delayedFlights) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
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

    public List<Q2DelayedFlight> getDelayedFlights() { return delayedFlights; }
    public void setDelayedFlights(List<Q2DelayedFlight> delayedFlights) { this.delayedFlights = delayedFlights; }
}