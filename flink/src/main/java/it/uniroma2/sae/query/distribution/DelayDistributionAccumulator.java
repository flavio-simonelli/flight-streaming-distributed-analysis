package it.uniroma2.sae.query.distribution;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.Store;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Accumulator for DelayDistributionQuery.
 * Uses Datadog's DDSketch to approximate percentiles with relative-error guarantees.
 * Implements custom writeObject/readObject serialization to safely persist DDSketch bins.
 */
public class DelayDistributionAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final double RELATIVE_ACCURACY = 0.01; // 1% relative accuracy

    private transient DDSketch sketch;

    public DelayDistributionAccumulator() {
        this.sketch = DDSketches.unboundedDense(RELATIVE_ACCURACY);
    }

    /**
     * Records a delay event into the DDSketch.
     */
    public void add(double val) {
        this.sketch.accept(val);
    }

    public long getCount() {
        return (long) this.sketch.getCount();
    }

    public double getMin() {
        return this.sketch.isEmpty() ? 0.0 : this.sketch.getMinValue();
    }

    public double getMax() {
        return this.sketch.isEmpty() ? 0.0 : this.sketch.getMaxValue();
    }

    /**
     * Estimates the given percentile from the DDSketch.
     */
    public double getPercentile(double q) {
        return this.sketch.isEmpty() ? 0.0 : this.sketch.getValueAtQuantile(q);
    }

    /**
     * Merges another DDSketch accumulator into this one.
     */
    public void merge(DelayDistributionAccumulator other) {
        this.sketch.mergeWith(other.sketch);
    }

    @Serial
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        out.defaultWriteObject();
        
        // Write zero count (read via reflection due to package-private access in getZeroCount)
        double zeroCount = 0.0;
        try {
            java.lang.reflect.Field field = sketch.getClass().getDeclaredField("zeroCount");
            field.setAccessible(true);
            zeroCount = field.getDouble(sketch);
        } catch (Exception e) {
            // Ignore/fallback
        }
        out.writeDouble(zeroCount);
        
        // Write index mapping accuracy
        out.writeDouble(RELATIVE_ACCURACY);

        // Serialize positive store
        Store positiveStore = sketch.getPositiveValueStore();
        List<Bin> positiveBins = new java.util.ArrayList<>();
        java.util.Iterator<?> posIt = positiveStore.getAscendingIterator();
        while (posIt.hasNext()) {
            positiveBins.add((com.datadoghq.sketch.ddsketch.store.Bin) posIt.next());
        }
        out.writeInt(positiveBins.size());
        for (com.datadoghq.sketch.ddsketch.store.Bin bin : positiveBins) {
            out.writeInt(bin.getIndex());
            out.writeDouble(bin.getCount());
        }

        // Serialize negative store
        com.datadoghq.sketch.ddsketch.store.Store negativeStore = sketch.getNegativeValueStore();
        java.util.List<com.datadoghq.sketch.ddsketch.store.Bin> negativeBins = new java.util.ArrayList<>();
        java.util.Iterator<?> negIt = negativeStore.getAscendingIterator();
        while (negIt.hasNext()) {
            negativeBins.add((com.datadoghq.sketch.ddsketch.store.Bin) negIt.next());
        }
        out.writeInt(negativeBins.size());
        for (com.datadoghq.sketch.ddsketch.store.Bin bin : negativeBins) {
            out.writeInt(bin.getIndex());
            out.writeDouble(bin.getCount());
        }
    }

    @Serial
    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();

        double zeroCount = in.readDouble();
        double accuracy = in.readDouble();
        
        this.sketch = DDSketches.unboundedDense(accuracy);
        if (zeroCount > 0) {
            try {
                java.lang.reflect.Field field = sketch.getClass().getDeclaredField("zeroCount");
                field.setAccessible(true);
                field.setDouble(sketch, zeroCount);
            } catch (Exception e) {
                // Ignore/fallback
            }
        }

        com.datadoghq.sketch.ddsketch.mapping.IndexMapping mapping = this.sketch.getIndexMapping();

        int positiveBinsCount = in.readInt();
        for (int i = 0; i < positiveBinsCount; i++) {
            int index = in.readInt();
            double count = in.readDouble();
            double val = mapping.value(index);
            this.sketch.accept(val, count);
        }

        int negativeBinsCount = in.readInt();
        for (int i = 0; i < negativeBinsCount; i++) {
            int index = in.readInt();
            double count = in.readDouble();
            double val = -mapping.value(index);
            this.sketch.accept(val, count);
        }
    }
}
