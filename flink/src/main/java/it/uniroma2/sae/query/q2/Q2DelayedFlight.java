package it.uniroma2.sae.query.q2;

import java.io.Serializable;

public class Q2DelayedFlight implements Serializable, Comparable<Q2DelayedFlight> {
    private static final long serialVersionUID = 1L;

    private String carrier;
    private String dest;
    private double depDelay;

    public Q2DelayedFlight() {}

    public Q2DelayedFlight(String carrier, String dest, double depDelay) {
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
    public int compareTo(Q2DelayedFlight o) {
        // Sort descending by depDelay
        return Double.compare(o.depDelay, this.depDelay);
    }

    @Override
    public String toString() {
        return String.format("(%s,%s,%.1f)", carrier, dest, depDelay);
    }
}
