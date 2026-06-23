package it.uniroma2.sae.query.rank;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Accumulator managing airport records and maintaining the top 20 delayed flights.
 */
public class RankAirportsAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final double LATE_THRESHOLD_MINUTES = 30.0;

    private int numFlights = 0;
    private int severeDelays = 0;
    private double sumDepDelay = 0.0;
    private double maxDepDelay = -Double.MAX_VALUE;

    private List<RankAirportsDelayedFlight> delayedFlights = new ArrayList<>();

    public RankAirportsAccumulator() {}

    public int getNumFlights() { return numFlights; }
    public void setNumFlights(int numFlights) { this.numFlights = numFlights; }

    public int getSevereDelays() { return severeDelays; }
    public void setSevereDelays(int severeDelays) { this.severeDelays = severeDelays; }

    public double getSumDepDelay() { return sumDepDelay; }
    public void setSumDepDelay(double sumDepDelay) { this.sumDepDelay = sumDepDelay; }

    public double getMaxDepDelay() { return numFlights == 0 ? 0.0 : maxDepDelay; }
    public void setMaxDepDelay(double maxDepDelay) { this.maxDepDelay = maxDepDelay; }

    public List<RankAirportsDelayedFlight> getDelayedFlights() { return delayedFlights; }
    public void setDelayedFlights(List<RankAirportsDelayedFlight> delayedFlights) { this.delayedFlights = delayedFlights; }

    public void add(String carrier, String dest, double depDelay) {
        this.numFlights++;
        this.sumDepDelay += depDelay;
        if (depDelay > this.maxDepDelay) {
            this.maxDepDelay = depDelay;
        }

        if (depDelay > LATE_THRESHOLD_MINUTES) {
            this.severeDelays++;

            RankAirportsDelayedFlight newFlight = new RankAirportsDelayedFlight(carrier, dest, depDelay);

            int index = 0;
            while (index < this.delayedFlights.size() &&
                    this.delayedFlights.get(index).compareTo(newFlight) <= 0) {
                index++;
            }

            // Insert in-place
            if (index < 20) {
                this.delayedFlights.add(index, newFlight);

                if (this.delayedFlights.size() > 20) {
                    this.delayedFlights.removeLast();
                }
            }
        }
    }

    public RankAirportsAccumulator merge(RankAirportsAccumulator other) {
        RankAirportsAccumulator merged = new RankAirportsAccumulator();
        merged.numFlights = this.numFlights + other.numFlights;
        merged.severeDelays = this.severeDelays + other.severeDelays;
        merged.sumDepDelay = this.sumDepDelay + other.sumDepDelay;
        merged.maxDepDelay = Math.max(this.maxDepDelay, other.maxDepDelay);


        List<RankAirportsDelayedFlight> listA = this.delayedFlights;
        List<RankAirportsDelayedFlight> listB = other.delayedFlights;
        merged.delayedFlights = new ArrayList<>(20);

        int i = 0, j = 0;

        while (merged.delayedFlights.size() < 20 && (i < listA.size() || j < listB.size())) {
            if (i < listA.size() && j < listB.size()) {

                if (listA.get(i).compareTo(listB.get(j)) <= 0) {
                    merged.delayedFlights.add(listA.get(i));
                    i++;
                } else {
                    merged.delayedFlights.add(listB.get(j));
                    j++;
                }
            } else if (i < listA.size()) {
                merged.delayedFlights.add(listA.get(i));
                i++;
            } else {
                merged.delayedFlights.add(listB.get(j));
                j++;
            }
        }

        return merged;
    }
}
