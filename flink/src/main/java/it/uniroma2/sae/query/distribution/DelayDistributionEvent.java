package it.uniroma2.sae.query.distribution;

import it.uniroma2.sae.model.FlightRecord;
import java.io.Serial;
import java.io.Serializable;

/**
 * Lightweight representation of a flight event containing only the fields required for Query 3.
 * Used to optimize memory usage, serialization overhead, and network shuffle size.
 */
public class DelayDistributionEvent implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String airline;
    private int scheduledDepartureHour;
    private double depDelay;

    public DelayDistributionEvent() {}

    public DelayDistributionEvent(FlightRecord event) {
        this.airline = event.getAirline();
        this.scheduledDepartureHour = event.getScheduledDepartureHour();
        this.depDelay = event.getDepDelay();
    }

    public DelayDistributionEvent(String airline, int scheduledDepartureHour, double depDelay) {
        this.airline = airline;
        this.scheduledDepartureHour = scheduledDepartureHour;
        this.depDelay = depDelay;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public int getScheduledDepartureHour() {
        return scheduledDepartureHour;
    }

    public void setScheduledDepartureHour(int scheduledDepartureHour) {
        this.scheduledDepartureHour = scheduledDepartureHour;
    }

    public double getDepDelay() {
        return depDelay;
    }

    public void setDepDelay(double depDelay) {
        this.depDelay = depDelay;
    }

    @Override
    public String toString() {
        return "DelayDistributionEvent{" +
                "airline='" + airline + '\'' +
                ", scheduledDepartureHour=" + scheduledDepartureHour +
                ", depDelay=" + depDelay +
                '}';
    }
}
