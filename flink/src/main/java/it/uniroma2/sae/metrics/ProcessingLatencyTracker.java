package it.uniroma2.sae.metrics;

import org.apache.flink.metrics.MetricGroup;

import java.io.Serial;
import java.io.Serializable;

/**
 * Registry helper that binds custom latency metrics (E2E latency and operator time)
 * to Flink's internal MetricGroup system using DDSketch accumulators.
 * <p>
 * Architectural Note: End-to-End latency measurements rely on sub-millisecond clock
 * synchronization across cluster nodes. This setup assumes EC2 instances run the
 * Amazon Time Sync Service (Chrony NTP) active by default on AWS, keeping clock skew
 * negligible. A local Math.max(0, latencyMs) check protects against minor clock anomalies.
 * </p>
 */
public class ProcessingLatencyTracker implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String name;
    private transient ProcessingLatencyAccumulator e2eAccumulator;
    private transient ProcessingLatencyAccumulator e2eMinAccumulator;
    private transient ProcessingLatencyAccumulator e2eAvgAccumulator;
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
        this.e2eMinAccumulator = new ProcessingLatencyAccumulator();
        this.e2eAvgAccumulator = new ProcessingLatencyAccumulator();
        this.operatorAccumulator = new ProcessingLatencyAccumulator();

        MetricGroup group = parentGroup.addGroup("processing_latency").addGroup(name);

        // Register Gauges for End-to-End Latency (ms) - Newest Tuple
        group.gauge("e2e_newest_ms_p50", () -> e2eAccumulator.getPercentile(0.50));
        group.gauge("e2e_newest_ms_p90", () -> e2eAccumulator.getPercentile(0.90));
        group.gauge("e2e_newest_ms_p95", () -> e2eAccumulator.getPercentile(0.95));
        group.gauge("e2e_newest_ms_p99", () -> e2eAccumulator.getPercentile(0.99));
        group.gauge("e2e_newest_ms_max", () -> e2eAccumulator.getMax());

        // Register Gauges for End-to-End Latency (ms) - Oldest Tuple
        group.gauge("e2e_oldest_ms_p50", () -> e2eMinAccumulator.getPercentile(0.50));
        group.gauge("e2e_oldest_ms_p90", () -> e2eMinAccumulator.getPercentile(0.90));
        group.gauge("e2e_oldest_ms_p95", () -> e2eMinAccumulator.getPercentile(0.95));
        group.gauge("e2e_oldest_ms_p99", () -> e2eMinAccumulator.getPercentile(0.99));
        group.gauge("e2e_oldest_ms_max", () -> e2eMinAccumulator.getMax());

        // Register Gauges for End-to-End Latency (ms) - Average Tuple Ingestion
        group.gauge("e2e_avg_ms_p50", () -> e2eAvgAccumulator.getPercentile(0.50));
        group.gauge("e2e_avg_ms_p90", () -> e2eAvgAccumulator.getPercentile(0.90));
        group.gauge("e2e_avg_ms_p95", () -> e2eAvgAccumulator.getPercentile(0.95));
        group.gauge("e2e_avg_ms_p99", () -> e2eAvgAccumulator.getPercentile(0.99));
        group.gauge("e2e_avg_ms_max", () -> e2eAvgAccumulator.getMax());

        // Register Gauges for Operator Processing Time (ms)
        group.gauge("operator_ms_p50", () -> operatorAccumulator.getPercentile(0.50));
        group.gauge("operator_ms_p90", () -> operatorAccumulator.getPercentile(0.90));
        group.gauge("operator_ms_p95", () -> operatorAccumulator.getPercentile(0.95));
        group.gauge("operator_ms_p99", () -> operatorAccumulator.getPercentile(0.99));
        group.gauge("operator_ms_max", () -> operatorAccumulator.getMax());
    }

    /**
     * Records a new End-to-End latency sample.
     * Assumes EC2 cluster clocks are synchronized via AWS Amazon Time Sync Service.
     *
     * @param systemIngestionTime the wall-clock time when the record entered Flink (ms)
     */
    public void updateE2E(long systemIngestionTime) {
        updateE2E(systemIngestionTime, systemIngestionTime, systemIngestionTime);
    }

    /**
     * Records a new End-to-End latency sample with three bounds.
     * Assumes EC2 cluster clocks are synchronized via AWS Amazon Time Sync Service.
     *
     * @param maxIngestion the maximum ingestion time (newest tuple)
     * @param minIngestion the minimum ingestion time (oldest tuple)
     * @param avgIngestion the average ingestion time
     */
    public void updateE2E(long maxIngestion, long minIngestion, long avgIngestion) {
        long now = System.currentTimeMillis();
        if (maxIngestion > 0 && e2eAccumulator != null) {
            long latencyMs = now - maxIngestion;
            e2eAccumulator.add(Math.max(0, latencyMs));
        }
        if (minIngestion > 0 && minIngestion != Long.MAX_VALUE && e2eMinAccumulator != null) {
            long latencyMs = now - minIngestion;
            e2eMinAccumulator.add(Math.max(0, latencyMs));
        }
        if (avgIngestion > 0 && e2eAvgAccumulator != null) {
            long latencyMs = now - avgIngestion;
            e2eAvgAccumulator.add(Math.max(0, latencyMs));
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
