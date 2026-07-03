package it.uniroma2.sae.query.distribution;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.CubicallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;

import java.io.Serial;
import java.io.Serializable;

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
    private static final int MAX_STORE_BINS = 4096;

    /**
     * Active quantile sketch structure.
     */
    private final DDSketch sketch;

    private long count;
    private double exactMin = Double.POSITIVE_INFINITY;
    private double exactMax = Double.NEGATIVE_INFINITY;
    public long maxSystemIngestionTime = 0L;
    public long minSystemIngestionTime = Long.MAX_VALUE;
    public long sumSystemIngestionTime = 0L;
    public long systemIngestionTimeCount = 0L;

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
        this.maxSystemIngestionTime = Math.max(this.maxSystemIngestionTime, other.maxSystemIngestionTime);
        if (other.minSystemIngestionTime != Long.MAX_VALUE) {
            this.minSystemIngestionTime = Math.min(this.minSystemIngestionTime, other.minSystemIngestionTime);
        }
        this.sumSystemIngestionTime += other.sumSystemIngestionTime;
        this.systemIngestionTimeCount += other.systemIngestionTimeCount;
    }
}
