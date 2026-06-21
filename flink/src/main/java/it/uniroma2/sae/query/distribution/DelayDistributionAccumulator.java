package it.uniroma2.sae.query.distribution;

import java.io.Serial;
import java.io.Serializable;

/**
 * Accumulator for DelayDistributionQuery.
 * Implements a hand-coded fixed-bucket histogram sketch to approximate percentiles in O(1) space.
 * Covers delays from -100 to 2000 minutes with 1-minute bins.
 */
public class DelayDistributionAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final int MIN_VAL = -100;
    private static final int MAX_VAL = 2000;
    private static final int NUM_BUCKETS = MAX_VAL - MIN_VAL + 1;

    private final long[] buckets = new long[NUM_BUCKETS];
    private long count = 0L;
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;

    public DelayDistributionAccumulator() {}

    /**
     * Records a delay event into the histogram.
     */
    public void add(double val) {
        this.count++;
        if (val < this.min) this.min = val;
        if (val > this.max) this.max = val;

        int bin = (int) Math.round(val);
        if (bin < MIN_VAL) {
            buckets[0]++;
        } else if (bin > MAX_VAL) {
            buckets[NUM_BUCKETS - 1]++;
        } else {
            buckets[bin - MIN_VAL]++;
        }
    }

    public long getCount() { return count; }
    public double getMin() { return count > 0 ? min : 0.0; }
    public double getMax() { return count > 0 ? max : 0.0; }

    /**
     * Estimates the given percentile from the cumulative histogram counts.
     */
    public double getPercentile(double q) {
        if (count == 0) return 0.0;

        double targetIndex = q * count;
        long cumulative = 0;

        for (int i = 0; i < NUM_BUCKETS; i++) {
            cumulative += buckets[i];
            if (cumulative >= targetIndex) {
                return MIN_VAL + i;
            }
        }
        return MAX_VAL;
    }

    /**
     * Merges another histogram accumulator into this one.
     */
    public void merge(DelayDistributionAccumulator other) {
        this.count += other.count;
        this.min = Math.min(this.min, other.min);
        this.max = Math.max(this.max, other.max);
        for (int i = 0; i < NUM_BUCKETS; i++) {
            this.buckets[i] += other.buckets[i];
        }
    }
}
