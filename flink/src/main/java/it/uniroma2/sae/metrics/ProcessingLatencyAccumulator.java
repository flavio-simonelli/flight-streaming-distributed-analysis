package it.uniroma2.sae.metrics;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.CubicallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;

import java.io.Serial;
import java.io.Serializable;

/**
 * Thread-safe accumulator utilizing DDSketch to track the distribution of latencies.
 * Accumulates observations for the entire run.
 */
public class ProcessingLatencyAccumulator implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final double RELATIVE_ACCURACY = 0.01;
    private static final int MAX_STORE_BINS = 1024;

    private transient DDSketch sketch;
    private double max = 0.0;

    public ProcessingLatencyAccumulator() {
        initSketch();
    }

    private void initSketch() {
        this.sketch = new DDSketch(
                new CubicallyInterpolatedMapping(RELATIVE_ACCURACY),
                () -> new CollapsingLowestDenseStore(MAX_STORE_BINS)
        );
    }

    /**
     * Records a new latency observation.
     *
     * @param val the observed value in milliseconds
     */
    public synchronized void add(double val) {
        if (sketch == null) {
            initSketch();
        }

        if (Double.isFinite(val) && val >= 0.0) {
            sketch.accept(val);
            if (val > max) {
                max = val;
            }
        }
    }

    /**
     * Resolves the target percentile value.
     *
     * @param q the percentile target (between 0.0 and 1.0)
     * @return the approximated latency in milliseconds
     */
    public synchronized double getPercentile(double q) {
        if (sketch == null || sketch.isEmpty()) {
            return 0.0;
        }
        return sketch.getValueAtQuantile(q);
    }

    /**
     * Resolves the maximum latency observed in the current rolling window.
     *
     * @return the maximum latency in milliseconds
     */
    public synchronized double getMax() {
        return max;
    }
}
