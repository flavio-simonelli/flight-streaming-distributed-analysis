package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.model.FlightRecord;
import java.io.Serial;
import java.io.Serializable;

/**
 * Lightweight representation of a flight event containing only the fields required for Query 1.
 * Used to optimize memory usage, serialization overhead, and network shuffle size.
 */
public class AirlinePerformanceEvent implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String airline;
    private boolean cancelled;
    private double depDelay;
    private boolean diverted;
    private long systemIngestionTime;

    public AirlinePerformanceEvent() {}

    public AirlinePerformanceEvent(FlightRecord event) {
        this.airline = event.getAirline();
        this.cancelled = event.isCancelled();
        this.depDelay = event.getDepDelay();
        this.diverted = event.isDiverted();
        this.systemIngestionTime = event.getSystemIngestionTime();
    }

    public AirlinePerformanceEvent(String airline, boolean cancelled, double depDelay, boolean diverted) {
        this.airline = airline;
        this.cancelled = cancelled;
        this.depDelay = depDelay;
        this.diverted = diverted;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public double getDepDelay() {
        return depDelay;
    }

    public void setDepDelay(double depDelay) {
        this.depDelay = depDelay;
    }

    public boolean isDiverted() {
        return diverted;
    }

    public void setDiverted(boolean diverted) {
        this.diverted = diverted;
    }

    public long getSystemIngestionTime() {
        return systemIngestionTime;
    }

    public void setSystemIngestionTime(long systemIngestionTime) {
        this.systemIngestionTime = systemIngestionTime;
    }

    @Override
    public String toString() {
        return "AirlinePerformanceEvent{" +
                "airline='" + airline + '\'' +
                ", cancelled=" + cancelled +
                ", depDelay=" + depDelay +
                ", diverted=" + diverted +
                '}';
    }
}
