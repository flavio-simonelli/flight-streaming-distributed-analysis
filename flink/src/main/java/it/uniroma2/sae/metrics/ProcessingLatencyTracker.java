package it.uniroma2.sae.metrics;

import org.apache.flink.metrics.MetricGroup;

import java.io.Serial;
import java.io.Serializable;

/**
 * Registry helper that binds custom latency metrics (E2E latency and operator time)
 * to Flink's internal MetricGroup system using DDSketch accumulators.
 */
public class ProcessingLatencyTracker implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String name;
    private transient ProcessingLatencyAccumulator e2eAccumulator;
    private transient ProcessingLatencyAccumulator operatorAccumulator;

    public ProcessingLatencyTracker(String name) {
        this.name = name;
    }

    /**
     * Registers the custom Gauges under Flink's MetricGroup registry namespace.
     *
     * @param parentGroup the root metric group of the context operator
     */
    public void register(MetricGroup parentGroup) {
        this.e2eAccumulator = new ProcessingLatencyAccumulator();
        this.operatorAccumulator = new ProcessingLatencyAccumulator();

        MetricGroup group = parentGroup.addGroup("processing_latency").addGroup(name);

        // Register Gauges for End-to-End Latency (ms)
        group.gauge("e2e_ms_p50", () -> e2eAccumulator.getPercentile(0.50));
        group.gauge("e2e_ms_p90", () -> e2eAccumulator.getPercentile(0.90));
        group.gauge("e2e_ms_p95", () -> e2eAccumulator.getPercentile(0.95));
        group.gauge("e2e_ms_p99", () -> e2eAccumulator.getPercentile(0.99));
        group.gauge("e2e_ms_max", () -> e2eAccumulator.getMax());

        // Register Gauges for Operator Processing Time (ms)
        group.gauge("operator_ms_p50", () -> operatorAccumulator.getPercentile(0.50));
        group.gauge("operator_ms_p90", () -> operatorAccumulator.getPercentile(0.90));
        group.gauge("operator_ms_p95", () -> operatorAccumulator.getPercentile(0.95));
        group.gauge("operator_ms_p99", () -> operatorAccumulator.getPercentile(0.99));
        group.gauge("operator_ms_max", () -> operatorAccumulator.getMax());
    }

    /**
     * Records a new End-to-End latency sample.
     * Safeguards against clock skew by enforcing a non-negative boundary.
     *
     * @param systemIngestionTime the wall-clock time when the record entered Flink (ms)
     */
    public void updateE2E(long systemIngestionTime) {
        if (systemIngestionTime > 0 && e2eAccumulator != null) {
            long latencyMs = System.currentTimeMillis() - systemIngestionTime;
            // Protect against clock skew (EC2 distributed setup)
            e2eAccumulator.add(Math.max(0, latencyMs));
        }
    }

    /**
     * Records a new operator execution time sample.
     * Safeguards against clock skew by enforcing a non-negative boundary.
     *
     * @param durationMs the elapsed operator duration directly in milliseconds
     */
    public void updateOperator(long durationMs) {
        if (operatorAccumulator != null) {
            // Protect against clock skew / timer anomalies
            operatorAccumulator.add(Math.max(0.0, durationMs));
        }
    }
}
