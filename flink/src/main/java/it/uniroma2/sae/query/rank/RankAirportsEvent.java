package it.uniroma2.sae.query.rank;

import it.uniroma2.sae.model.FlightRecord;
import java.io.Serial;
import java.io.Serializable;

/**
 * Lightweight representation of a flight event containing only the fields required for Query 2.
 * Used to optimize memory usage, serialization overhead, and network shuffle size.
 */
public class RankAirportsEvent implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private int originAirportId;
    private String airline;
    private int destinationAirportId;
    private double depDelay;

    public RankAirportsEvent() {}

    public RankAirportsEvent(FlightRecord event) {
        this.originAirportId = event.getOriginAirportId();
        this.airline = event.getAirline();
        this.destinationAirportId = event.getDestinationAirportId();
        this.depDelay = event.getDepDelay();
    }

    public RankAirportsEvent(int originAirportId, String airline, int destinationAirportId, double depDelay) {
        this.originAirportId = originAirportId;
        this.airline = airline;
        this.destinationAirportId = destinationAirportId;
        this.depDelay = depDelay;
    }

    public int getOriginAirportId() {
        return originAirportId;
    }

    public void setOriginAirportId(int originAirportId) {
        this.originAirportId = originAirportId;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public int getDestinationAirportId() {
        return destinationAirportId;
    }

    public void setDestinationAirportId(int destinationAirportId) {
        this.destinationAirportId = destinationAirportId;
    }

    public double getDepDelay() {
        return depDelay;
    }

    public void setDepDelay(double depDelay) {
        this.depDelay = depDelay;
    }

    @Override
    public String toString() {
        return "RankAirportsEvent{" +
                "originAirportId=" + originAirportId +
                ", airline='" + airline + '\'' +
                ", destinationAirportId=" + destinationAirportId +
                ", depDelay=" + depDelay +
                '}';
    }
}
