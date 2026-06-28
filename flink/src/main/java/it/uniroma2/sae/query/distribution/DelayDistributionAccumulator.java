package it.uniroma2.sae.query.distribution;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.CubicallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Mutable accumulator used to approximate delay distributions through a DDSketch.
 *
 * <p>
 * This accumulator is designed for streaming aggregation scenarios where keeping all raw
 * delay samples in memory would be too expensive. DDSketch provides approximate quantile
 * queries with relative-error guarantees while using a compact indexed-bin representation.
 * </p>
 *
 * <p>
 * The internal {@link DDSketch} instance is marked as transient because its internal state
 * is manually serialized. The custom serialization logic stores only the positive bins,
 * negative bins, zero count and accuracy configuration. This makes the accumulator safer
 * and more explicit when it is transferred across Flink operators or persisted in state
 * checkpoints.
 * </p>
 */
public class DelayDistributionAccumulator implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Serialization format version.
     *
     * <p>
     * This field allows future changes to the custom binary layout without silently breaking
     * compatibility with older checkpoints or serialized accumulator instances.
     * </p>
     */
    private static final int SERIALIZATION_VERSION = 1;

    /**
     * Relative accuracy used by the DDSketch.
     *
     * <p>
     * A value of {@code 0.01} means that quantile values are approximated with about 1%
     * relative error.
     * </p>
     */
    private static final double RELATIVE_ACCURACY = 0.01;

    /**
     * Transient DDSketch instance used to store and query the delay distribution.
     *
     * <p>
     * It is transient because the default Java serialization of DDSketch is intentionally
     * avoided. Instead, only the relevant bin topology is serialized manually.
     * </p>
     */
    private transient DDSketch sketch;

    /**
     * Creates a new empty delay distribution accumulator.
     */
    public DelayDistributionAccumulator() {
        this.sketch = newSketch();
    }

    /**
     * Creates a new DDSketch instance using the accumulator default configuration.
     *
     * @return a new empty DDSketch
     */
    private static DDSketch newSketch() {
        return new DDSketch(
                new CubicallyInterpolatedMapping(RELATIVE_ACCURACY),
                UnboundedSizeDenseStore::new
        );
    }

    /**
     * Adds a delay sample to the distribution.
     *
     * <p>
     * Non-finite values such as {@code NaN}, {@code +Infinity} and {@code -Infinity} are ignored
     * because they cannot be represented meaningfully inside the quantile sketch.
     * </p>
     *
     * @param val delay value to add, expressed in minutes
     */
    public void add(double val) {
        if (!Double.isFinite(val)) {
            return;
        }

        this.sketch.accept(val);
    }

    /**
     * Returns the number of samples currently represented by this accumulator.
     *
     * @return total number of accumulated samples
     */
    public long getCount() {
        return Math.round(this.sketch.getCount());
    }

    /**
     * Returns the minimum observed delay value.
     *
     * @return minimum delay value, or {@code 0.0} if the accumulator is empty
     */
    public double getMin() {
        return this.sketch.isEmpty() ? 0.0 : this.sketch.getMinValue();
    }

    /**
     * Returns the maximum observed delay value.
     *
     * @return maximum delay value, or {@code 0.0} if the accumulator is empty
     */
    public double getMax() {
        return this.sketch.isEmpty() ? 0.0 : this.sketch.getMaxValue();
    }

    /**
     * Returns the approximate value at the requested quantile.
     *
     * <p>
     * The quantile must be in the range {@code [0.0, 1.0]}. For example:
     * </p>
     *
     * <ul>
     *     <li>{@code 0.50} returns the median</li>
     *     <li>{@code 0.75} returns the 75th percentile</li>
     *     <li>{@code 0.95} returns the 95th percentile</li>
     * </ul>
     *
     * @param q target quantile in the range {@code [0.0, 1.0]}
     * @return approximate value at the requested quantile, or {@code 0.0} if empty
     * @throws IllegalArgumentException if {@code q} is outside the valid range
     */
    public double getPercentile(double q) {
        if (q < 0.0 || q > 1.0) {
            throw new IllegalArgumentException("Percentile must be between 0.0 and 1.0");
        }

        return this.sketch.isEmpty() ? 0.0 : this.sketch.getValueAtQuantile(q);
    }

    /**
     * Merges another accumulator into this accumulator.
     *
     * <p>
     * This method is useful in distributed aggregation pipelines, where partial sketches
     * are created independently and then combined during reduce or window aggregation phases.
     * </p>
     *
     * @param other accumulator to merge into this one
     */
    public void merge(DelayDistributionAccumulator other) {
        if (other == null || other.sketch == null || other.sketch.isEmpty()) {
            return;
        }

        this.sketch.mergeWith(other.sketch);
    }

    /**
     * Serializes this accumulator using a compact custom binary format.
     *
     * <p>
     * The method stores:
     * </p>
     *
     * <ol>
     *     <li>serialization format version</li>
     *     <li>DDSketch relative accuracy</li>
     *     <li>number of zero-valued samples</li>
     *     <li>positive value store bins</li>
     *     <li>negative value store bins</li>
     * </ol>
     *
     * <p>
     * This avoids relying on DDSketch private fields or default Java serialization.
     * </p>
     *
     * @param out output stream used by Java serialization
     * @throws IOException if writing to the stream fails
     */
    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        out.writeInt(SERIALIZATION_VERSION);
        out.writeDouble(RELATIVE_ACCURACY);

        Store positiveStore = sketch.getPositiveValueStore();
        Store negativeStore = sketch.getNegativeValueStore();

        double zeroCount = computeZeroCount(sketch, positiveStore, negativeStore);
        out.writeDouble(zeroCount);

        serializeStore(positiveStore, out);
        serializeStore(negativeStore, out);
    }

    /**
     * Restores this accumulator from the custom binary serialization format.
     *
     * <p>
     * The method rebuilds the DDSketch by reconstructing its index mapping, positive store,
     * negative store and zero count. This preserves the original bin structure without
     * replaying raw values through {@link DDSketch#accept(double)}.
     * </p>
     *
     * @param in input stream used by Java deserialization
     * @throws IOException if the serialized format is invalid or reading fails
     * @throws ClassNotFoundException if Java default deserialization cannot resolve a class
     */
    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        int version = in.readInt();
        if (version != SERIALIZATION_VERSION) {
            throw new IOException("Unsupported DelayDistributionAccumulator serialization version: " + version);
        }

        double accuracy = in.readDouble();
        double zeroCount = in.readDouble();

        Store positiveStore = new UnboundedSizeDenseStore();
        Store negativeStore = new UnboundedSizeDenseStore();

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

    /**
     * Computes the number of zero-valued samples represented by the sketch.
     *
     * <p>
     * DDSketch stores positive values, negative values and zero values separately. Since the
     * public store iterators expose only positive and negative bins, the zero count can be
     * derived as:
     * </p>
     *
     * <pre>
     * zeroCount = totalCount - positiveCount - negativeCount
     * </pre>
     *
     * @param sketch sketch whose total count is used
     * @param positiveStore positive value store
     * @param negativeStore negative value store
     * @return estimated zero count, never negative
     */
    private static double computeZeroCount(DDSketch sketch, Store positiveStore, Store negativeStore) {
        double nonZeroCount = sumStoreCounts(positiveStore) + sumStoreCounts(negativeStore);
        return Math.max(0.0, sketch.getCount() - nonZeroCount);
    }

    /**
     * Computes the sum of all bin weights contained in a store.
     *
     * @param store store to inspect
     * @return total count represented by the store
     */
    private static double sumStoreCounts(Store store) {
        double sum = 0.0;

        Iterator<Bin> iterator = store.getAscendingIterator();
        while (iterator.hasNext()) {
            sum += iterator.next().getCount();
        }

        return sum;
    }

    /**
     * Serializes all active bins from a DDSketch store.
     *
     * <p>
     * Each bin is written as an index-count pair:
     * </p>
     *
     * <pre>
     * int    binIndex
     * double binCount
     * </pre>
     *
     * @param store store to serialize
     * @param out output stream
     * @throws IOException if writing to the stream fails
     */
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

    /**
     * Counts the number of active bins in a store.
     *
     * @param store store to inspect
     * @return number of active bins
     */
    private static int countBins(Store store) {
        int count = 0;

        Iterator<Bin> iterator = store.getAscendingIterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }

        return count;
    }

    /**
     * Deserializes a DDSketch store from the custom binary format.
     *
     * <p>
     * The input stream is expected to contain first the number of bins, followed by one
     * index-count pair for each bin.
     * </p>
     *
     * @param targetStore store to populate
     * @param in input stream
     * @throws IOException if the serialized format is invalid or reading fails
     */
    private static void deserializeStore(Store targetStore, ObjectInputStream in) throws IOException {
        int binCount = in.readInt();

        if (binCount < 0) {
            throw new IOException("Invalid DDSketch bin count: " + binCount);
        }

        for (int i = 0; i < binCount; i++) {
            int index = in.readInt();
            double count = in.readDouble();

            if (count < 0.0 || Double.isNaN(count)) {
                throw new IOException("Invalid DDSketch bin weight: " + count);
            }

            targetStore.add(index, count);
        }
    }
}
