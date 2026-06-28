package it.uniroma2.sae.query.distribution;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.CubicallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.datadoghq.sketch.ddsketch.store.Store;

import java.io.*;
import java.util.Iterator;

/**
 * Mutable accumulator component used to approximate delay distributions through a DDSketch structure.
 * <p>
 * This class is specifically engineered for streaming aggregation scenarios where retaining
 * raw delay samples in memory would be computationally prohibitive. DDSketch provides bounded
 * relative-error guarantees for quantile estimation while maintaining a highly compact
 * indexed-bin memory representation.
 * </p>
 * <p>
 * The internal sketching topology is intentionally decoupled from Java's standard serialization
 * mechanism by using transient fields and explicit manual binary serialization routines. This architecture
 * guarantees state safety and deterministic snapshots during Flink operator checkpoints and scale-out tasks.
 * </p>
 */
public class DelayDistributionAccumulator implements Serializable {

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
     * anomalous delay records, collapsing distant outliers into the lowest/highest boundary bins.
     * </p>
     * Use:
     * <ul>
     *     <li>{@code 512}: low-memory mode, acceptable when extreme percentiles are not critical.</li>
     *     <li>{@code 1024}: balanced setting for compact state.</li>
     *     <li>{@code 2048}: safer default for this query and delay-oriented percentiles.</li>
     *     <li>{@code 4096}: useful only when the value range is very large or collapsing must be minimized.</li>
     * </ul>
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
     * Constructs a new empty {@code DelayDistributionAccumulator} initializing an isolated sketch instance.
     */
    public DelayDistributionAccumulator() {
        this.sketch = newSketch();
    }

    /**
     * Creates a new DDSketch configured for bounded-memory delay quantile estimation.
     * <p>
     * The sketch uses a cubically interpolated index mapping to provide relative-error
     * guarantees for percentile queries, while {@link CollapsingLowestDenseStore} limits
     * the maximum number of allocated bins. This prevents unbounded memory growth when
     * anomalous or extremely distant delay values are observed in the stream.
     * </p>
     * <p>
     * The lowest bins are collapsed first, preserving better resolution on higher delay
     * values, which are typically more relevant for delay-distribution analysis.
     * </p>
     *
     * @return an empty DDSketch configured with bounded dense stores
     */
    private static DDSketch newSketch() {
        return new DDSketch(
                new CubicallyInterpolatedMapping(RELATIVE_ACCURACY),
                // Use CollapsingLowestDenseStore instead of UnboundedSizeDenseStore to cap
                // the number of allocated bins. The unbounded store preserves all bins but
                // may grow excessively with extreme outliers; the collapsing store bounds
                // memory usage by merging the lowest bins first.
                () -> new CollapsingLowestDenseStore(MAX_STORE_BINS)
        );
    }

    /**
     * Injects a raw departure delay sample into the current tracking distribution sketch.
     * Safe-guards internal boundaries by automatically discarding non-finite anomalies (NaN/Infinities).
     *
     * @param val the departure delay value to register, expressed in minutes
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
     * @return the rounded total count of accumulated flight record observations
     */
    public long getCount() {
        return this.count;
    }

    /**
     * Computes the absolute minimum operational delay value observed since window initialization.
     *
     * @return the minimum tracked delay value, or 0.0 if the distribution contains no data
     */
    public double getMin() {
        return this.count == 0 ? 0.0 : this.exactMin;
    }

    /**
     * Computes the absolute maximum operational delay value observed since window initialization.
     *
     * @return the maximum tracked delay value, or 0.0 if the distribution contains no data
     */
    public double getMax() {
        return this.count == 0 ? 0.0 : this.exactMax;
    }

    /**
     * Computes an approximate value matching the target percentile index using relative-error mapping.
     * Valid input bounds require a strictly normalized probability range between 0.0 and 1.0.
     *
     * @param q the mathematical quantile targeted for resolution (e.g., 0.50 for median, 0.90 for 90th percentile)
     * @return the approximated operational delay value matching the target percentile, or 0.0 if empty
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
     * This operational logic is vital for multi-stage network mergers during partial window reduces.
     *
     * @param other the independent remote accumulator state targeted for local integration
     */
    public void merge(DelayDistributionAccumulator other) {
        if (other == null || other.sketch == null || other.sketch.isEmpty()) {
            return;
        }
        this.sketch.mergeWith(other.sketch);

        this.count += other.count;
        this.exactMin = Math.min(this.exactMin, other.exactMin);
        this.exactMax = Math.max(this.exactMax, other.exactMax);
    }

    /* --- SERIALIZATION --- */

    /**
     * Serializes the current internal state footprint into a highly compact, specialized binary format.
     * Sequentially writes out structural metadata, accuracy parameters, and dense array bins.
     *
     * @param out the destination object stream assigned by the JVM runtime environment
     * @throws IOException if network, memory, or disk-bound IO failures halt serialization
     */
    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        // Write the structural serialization version and accuracy configuration
        out.writeInt(SERIALIZATION_VERSION);
        out.writeDouble(RELATIVE_ACCURACY);

        Store positiveStore = sketch.getPositiveValueStore();
        Store negativeStore = sketch.getNegativeValueStore();

        // Deduce the independent zero-bound balance to protect sparse indexing
        double zeroCount = computeZeroCount(sketch, positiveStore, negativeStore);
        out.writeDouble(zeroCount);

        // Stream down data blocks belonging to both positive and negative stores
        serializeStore(positiveStore, out);
        serializeStore(negativeStore, out);
    }

    /**
     * Reconstructs the exact sketching structure using custom binary deserialization protocols.
     * Re-establishes the core mathematical indexes without replaying raw events through the execution graph.
     *
     * @param in the source object input stream assigned by the JVM runtime environment
     * @throws IOException if the incoming byte configuration layout breaks structural consistency rules
     * @throws ClassNotFoundException if reference lookups fail during standard metadata unpacks
     */
    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        int version = in.readInt();
        if (version != SERIALIZATION_VERSION) {
            throw new IOException("Unsupported DelayDistributionAccumulator serialization version: " + version);
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

        // Dynamically unpack and restore structural store dimensions
        deserializeStore(positiveStore, in);
        deserializeStore(negativeStore, in);

        IndexMapping mapping = new CubicallyInterpolatedMapping(accuracy);

        // Reconstitute the unified DDSketch structure using recovered parameters
        this.sketch = DDSketch.of(
                mapping,
                negativeStore,
                positiveStore,
                zeroCount
        );
    }

    /**
     * Intercepts and isolates the count of exact zero-valued elements handled by the pipeline.
     * Extrapolates values by subtracting active store weights from the global sample counter.
     *
     * @param sketch the primary sketch instance holding global execution parameters
     * @param positiveStore the internal store array handling strictly positive values
     * @param negativeStore the internal store array handling strictly negative values
     * @return the computed sum of non-delayed neutral zero entries
     */
    private static double computeZeroCount(DDSketch sketch, Store positiveStore, Store negativeStore) {
        double nonZeroCount = positiveStore.getTotalCount() + negativeStore.getTotalCount();
        return Math.max(0.0, sketch.getCount() - nonZeroCount);
    }

    /**
     * Packs active buckets from an individual structural store directly onto the downstream serialized stream.
     * Serializes layout bounds by emitting index-count pairs explicitly.
     *
     * @param store the specific target structural store to compress and export
     * @param out the open output byte stream targeted for injection
     * @throws IOException if data writing operations face structural interrupts
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
     * Counts the total number of non-empty indexed buckets populated inside a specific store.
     *
     * @param store the specific store targeted for inspection
     * @return the absolute integer count of active storage bins
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
     * Deserializes and re-populates an un-structured store instance from incoming custom stream records.
     * Asserts data quality criteria on individual bin weights to safeguard state layout consistency.
     *
     * @param targetStore the allocated empty store destined to receive recovered bins
     * @param in the source input stream reading binary packages
     * @throws IOException if index bounds are corrupted or negative frequency metrics are intercepted
     */
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
