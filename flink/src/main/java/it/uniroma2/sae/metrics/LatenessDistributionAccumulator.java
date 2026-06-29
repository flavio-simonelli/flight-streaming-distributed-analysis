package it.uniroma2.sae.metrics;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.CubicallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.datadoghq.sketch.ddsketch.store.Store;

import java.io.*;
import java.util.Iterator;

/**
 * Mutable accumulator component used to approximate late record event-time lateness distributions
 * through a DDSketch structure.
 * <p>
 * This class is designed for streaming aggregation scenarios where retaining raw lateness samples in memory
 * would be computationally prohibitive. DDSketch provides bounded relative-error guarantees for quantile
 * estimation while maintaining a highly compact representation.
 * </p>
 * <p>
 * The internal sketching topology is decoupled from Java's standard serialization mechanism by using
 * transient fields and explicit manual binary serialization routines. This architecture guarantees state safety
 * and deterministic snapshots during Flink operator checkpoints and scale-out tasks.
 * </p>
 */
public class LatenessDistributionAccumulator implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Internal structural version used to validate binary compatibility during state deserialization. */
    private static final int SERIALIZATION_VERSION = 1;

    /**
     * The target relative accuracy bound applied to the underlying mapping.
     * A value of 0.01 limits the maximum approximation error to approximately 1% of the true quantile value.
     */
    private static final double RELATIVE_ACCURACY = 0.01;

    /**
     * Maximum number of bins allowed in the underlying sketch stores.
     * Bounding the store size (e.g., 2048) prevents OutOfMemory crashes in case of extremely
     * anomalous lateness records, collapsing distant outliers into the lowest/highest boundary bins.
     */
    private static final int MAX_STORE_BINS = 2048;

    /**
     * Transient wrapper containing the active quantile sketch structure.
     * Marked transient to explicitly override Java serialization in favor of a low-overhead custom layout.
     */
    private transient DDSketch sketch;

    private long count;
    private double exactMin = Double.POSITIVE_INFINITY;
    private double exactMax = Double.NEGATIVE_INFINITY;

    /**
     * Constructs a new empty {@code LatenessDistributionAccumulator} initializing an isolated sketch instance.
     */
    public LatenessDistributionAccumulator() {
        this.sketch = newSketch();
    }

    /**
     * Creates a new DDSketch configured for bounded-memory lateness quantile estimation.
     *
     * @return an empty DDSketch configured with bounded dense stores
     */
    private static DDSketch newSketch() {
        return new DDSketch(
                new CubicallyInterpolatedMapping(RELATIVE_ACCURACY),
                () -> new CollapsingLowestDenseStore(MAX_STORE_BINS)
        );
    }

    /**
     * Injects a raw lateness sample into the current tracking distribution sketch.
     *
     * @param val the lateness value to register, expressed in minutes
     */
    public void add(double val) {
        if (!Double.isFinite(val)) {
            return;
        }
        this.sketch.accept(val);

        this.count++;
        this.exactMin = Math.min(this.exactMin, val);
        this.exactMax = Math.max(this.exactMax, val);
    }

    /**
     * Retrieves the absolute number of observations successfully tracked within this accumulator.
     *
     * @return the total count of accumulated lateness observations
     */
    public long getCount() {
        return this.count;
    }

    /**
     * Computes the absolute minimum lateness value observed.
     *
     * @return the minimum tracked lateness value, or 0.0 if empty
     */
    public double getMin() {
        return this.count == 0 ? 0.0 : this.exactMin;
    }

    /**
     * Computes the absolute maximum lateness value observed.
     *
     * @return the maximum tracked lateness value, or 0.0 if empty
     */
    public double getMax() {
        return this.count == 0 ? 0.0 : this.exactMax;
    }

    /**
     * Computes an approximate value matching the target percentile index using relative-error mapping.
     *
     * @param q the mathematical quantile targeted for resolution (e.g., 0.50 for median, 0.90 for 90th percentile)
     * @return the approximated lateness value matching the target percentile, or 0.0 if empty
     * @throws IllegalArgumentException if the provided quantile variable falls outside the [0.0, 1.0] boundary
     */
    public double getPercentile(double q) {
        if (q < 0.0 || q > 1.0) {
            throw new IllegalArgumentException("Percentile must be between 0.0 and 1.0");
        }
        return this.sketch.isEmpty() ? 0.0 : this.sketch.getValueAtQuantile(q);
    }

    /**
     * Combines the state of another sub-accumulator directly into this sketch instance.
     *
     * @param other the independent remote accumulator state targeted for local integration
     */
    public void merge(LatenessDistributionAccumulator other) {
        if (other == null || other.sketch == null || other.sketch.isEmpty()) {
            return;
        }
        this.sketch.mergeWith(other.sketch);

        this.count += other.count;
        this.exactMin = Math.min(this.exactMin, other.exactMin);
        this.exactMax = Math.max(this.exactMax, other.exactMax);
    }

    /* --- SERIALIZATION --- */

    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        // Write serialization metadata
        out.writeInt(SERIALIZATION_VERSION);
        out.writeDouble(RELATIVE_ACCURACY);

        Store positiveStore = sketch.getPositiveValueStore();
        Store negativeStore = sketch.getNegativeValueStore();

        double zeroCount = computeZeroCount(sketch, positiveStore, negativeStore);
        out.writeDouble(zeroCount);

        serializeStore(positiveStore, out);
        serializeStore(negativeStore, out);
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        int version = in.readInt();
        if (version != SERIALIZATION_VERSION) {
            throw new IOException("Unsupported LatenessDistributionAccumulator serialization version: " + version);
        }

        double accuracy = in.readDouble();
        if (!Double.isFinite(accuracy) || accuracy <= 0.0 || accuracy >= 1.0) {
            throw new IOException("Invalid DDSketch relative accuracy: " + accuracy);
        }

        double zeroCount = in.readDouble();
        if (!Double.isFinite(zeroCount) || zeroCount < 0.0) {
            throw new IOException("Invalid DDSketch zero count: " + zeroCount);
        }

        Store positiveStore = new CollapsingLowestDenseStore(MAX_STORE_BINS);
        Store negativeStore = new CollapsingLowestDenseStore(MAX_STORE_BINS);

        deserializeStore(positiveStore, in);
        deserializeStore(negativeStore, in);

        IndexMapping mapping = new CubicallyInterpolatedMapping(accuracy);

        this.sketch = DDSketch.of(
                mapping,
                negativeStore,
                positiveStore,
                zeroCount
        );
    }

    private static double computeZeroCount(DDSketch sketch, Store positiveStore, Store negativeStore) {
        double nonZeroCount = positiveStore.getTotalCount() + negativeStore.getTotalCount();
        return Math.max(0.0, sketch.getCount() - nonZeroCount);
    }

    private static void serializeStore(Store store, ObjectOutputStream out) throws IOException {
        int size = countBins(store);
        out.writeInt(size);

        Iterator<Bin> iterator = store.getAscendingIterator();
        while (iterator.hasNext()) {
            Bin bin = iterator.next();
            out.writeInt(bin.getIndex());
            out.writeDouble(bin.getCount());
        }
    }

    private static int countBins(Store store) {
        int count = 0;
        Iterator<Bin> iterator = store.getAscendingIterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    private static void deserializeStore(Store targetStore, ObjectInputStream in) throws IOException {
        int binCount = in.readInt();

        if (binCount < 0 || binCount > MAX_STORE_BINS) {
            throw new IOException("Invalid DDSketch bin count: " + binCount);
        }

        for (int i = 0; i < binCount; i++) {
            int index = in.readInt();
            double count = in.readDouble();

            if (!Double.isFinite(count) || count < 0.0) {
                throw new IOException("Invalid DDSketch bin weight: " + count);
            }

            targetStore.add(index, count);
        }
    }
}
