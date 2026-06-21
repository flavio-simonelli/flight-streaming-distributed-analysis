package it.uniroma2.sae.query.rank;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serial;
import java.io.Serializable;

/**
 * Details of a delayed flight event.
 * Sorted in descending order of departure delay.
 */
public class RankAirportsDelayedFlight implements Serializable, Comparable<RankAirportsDelayedFlight> {
    @Serial
    private static final long serialVersionUID = 1L;

    @JsonProperty("carrier")
    private String carrier;

    @JsonProperty("dest")
    private String dest;

    @JsonProperty("dep_delay")
    private double depDelay;

    public RankAirportsDelayedFlight() {}

    public RankAirportsDelayedFlight(String carrier, String dest, double depDelay) {
        this.carrier = carrier;
        this.dest = dest;
        this.depDelay = depDelay;
    }

    public String getCarrier() { return carrier; }
    public void setCarrier(String carrier) { this.carrier = carrier; }

    public String getDest() { return dest; }
    public void setDest(String dest) { this.dest = dest; }

    public double getDepDelay() { return depDelay; }
    public void setDepDelay(double depDelay) { this.depDelay = depDelay; }

    @Override
    public int compareTo(RankAirportsDelayedFlight o) {
        return Double.compare(o.depDelay, this.depDelay);
    }

    @Override
    public String toString() {
        return String.format("(%s,%s,%.1f)", carrier, dest, depDelay);
    }
}
