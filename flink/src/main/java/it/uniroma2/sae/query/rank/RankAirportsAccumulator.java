package it.uniroma2.sae.query.rank;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Accumulator managing airport records and maintaining the top 20 delayed flights.
 */
public class RankAirportsAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private int numFlights = 0;
    private int severeDelays = 0;
    private double sumDepDelay = 0.0;
    private double maxDepDelay = 0.0;
    private List<RankAirportsDelayedFlight> delayedFlights = new ArrayList<>();

    public RankAirportsAccumulator() {}

    public int getNumFlights() { return numFlights; }
    public void setNumFlights(int numFlights) { this.numFlights = numFlights; }

    public int getSevereDelays() { return severeDelays; }
    public void setSevereDelays(int severeDelays) { this.severeDelays = severeDelays; }

    public double getSumDepDelay() { return sumDepDelay; }
    public void setSumDepDelay(double sumDepDelay) { this.sumDepDelay = sumDepDelay; }

    public double getMaxDepDelay() { return maxDepDelay; }
    public void setMaxDepDelay(double maxDepDelay) { this.maxDepDelay = maxDepDelay; }

    public List<RankAirportsDelayedFlight> getDelayedFlights() { return delayedFlights; }
    public void setDelayedFlights(List<RankAirportsDelayedFlight> delayedFlights) { this.delayedFlights = delayedFlights; }

    public void add(String carrier, String dest, double depDelay) {
        this.numFlights++;
        this.sumDepDelay += depDelay;
        if (depDelay > this.maxDepDelay) {
            this.maxDepDelay = depDelay;
        }
        if (depDelay > 30.0) {
            this.severeDelays++;
            this.delayedFlights.add(new RankAirportsDelayedFlight(carrier, dest, depDelay));
            Collections.sort(this.delayedFlights);
            if (this.delayedFlights.size() > 20) {
                this.delayedFlights.remove(this.delayedFlights.size() - 1);
            }
        }
    }

    public RankAirportsAccumulator merge(RankAirportsAccumulator other) {
        RankAirportsAccumulator merged = new RankAirportsAccumulator();
        merged.numFlights = this.numFlights + other.numFlights;
        merged.severeDelays = this.severeDelays + other.severeDelays;
        merged.sumDepDelay = this.sumDepDelay + other.sumDepDelay;
        merged.maxDepDelay = Math.max(this.maxDepDelay, other.maxDepDelay);
        merged.delayedFlights.addAll(this.delayedFlights);
        merged.delayedFlights.addAll(other.delayedFlights);
        Collections.sort(merged.delayedFlights);
        if (merged.delayedFlights.size() > 20) {
            merged.delayedFlights.subList(20, merged.delayedFlights.size()).clear();
        }
        return merged;
    }
}
