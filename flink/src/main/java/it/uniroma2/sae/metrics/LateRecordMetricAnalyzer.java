package it.uniroma2.sae.metrics;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * A custom {@link ProcessFunction} designed to analyze, track, and report the event-time
 * lateness distribution of delayed {@link FlightRecord} events.
 * <p>
 * This analyzer intercepts late-arriving elements routed through a side output, computing
 * their logical event-time delay relative to the window's final deadline. It registers and
 * exposes lightweight, highly optimized Flink metrics to monitor streaming health without
 * the memory overhead of windowed histograms.
 * </p>
 */
public class LateRecordMetricAnalyzer extends ProcessFunction<FlightRecord, FlightRecord> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** The size of the tumbling event-time window expressed in milliseconds. */
    private final long windowSizeMs;

    /** The allowed lateness buffer configuration expressed in milliseconds. */
    private final long allowedLatenessMs;

    /** Flink metric counter used to accumulate the total number of late records encountered. */
    private transient Counter totalLateRecordsCounter;

    /** Transient state tracking the absolute maximum event-time delay observed since application startup. */
    private transient long maxLatenessMinutes = 0;

    /**
     * Constructs a new {@code LateRecordMetricAnalyzer} with specified window parameters.
     *
     * @param windowSize      the duration of the logical tumbling window
     * @param allowedLateness the duration of the allowed lateness buffer configured for the window
     */
    public LateRecordMetricAnalyzer(Duration windowSize, Duration allowedLateness) {
        this.windowSizeMs = windowSize.toMillis();
        this.allowedLatenessMs = allowedLateness.toMillis();
    }

    /**
     * Initializes the process function and registers custom metrics within Flink's runtime context.
     * <p>
     * This method provisions a cumulative {@link Counter} for the aggregate late record volume
     * and a custom {@link org.apache.flink.metrics.Gauge} to report the monotonic historical peak lateness.
     * </p>
     *
     * @param context the runtime open context provided by the Flink ecosystem
     * @throws Exception if an error occurs during metric group registration
     */
    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext context) throws Exception {
        super.open(context);

        // Register the cumulative total late record counter
        this.totalLateRecordsCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("total_late_records_count");

        // Register the Gauge for monitoring the absolute maximum lateness peak
        getRuntimeContext()
                .getMetricGroup()
                .gauge("max_lateness_minutes_absolute", () -> maxLatenessMinutes);
    }

    /**
     * Processes each late-arriving flight record, evaluating its event-time delay against the window deadline.
     * <p>
     * The logical delay is computed by subtracting the window's maximum closing deadline
     * ({@code windowEnd + allowedLateness}) from the current input watermark. If a positive lateness
     * is detected, metrics are updated accordingly. The element is always forwarded to ensure downstream availability.
     * </p>
     *
     * @param record the incoming late flight record to evaluate
     * @param ctx    the execution context providing access to elements, timestamps, and timers
     * @param out    the collector used to forward the record down the streaming topology
     * @throws Exception if any internal aggregation or metric update fails
     */
    @Override
    public void processElement(FlightRecord record, Context ctx, Collector<FlightRecord> out) throws Exception {
        Long eventTime = ctx.timestamp();
        if (eventTime == null) {
            out.collect(record);
            return;
        }

        // Fetch the current progress of the logical event-time clock (Watermark)
        long watermark = ctx.timerService().currentWatermark();

        // Reconstruct the boundary parameters of the target window
        long windowStart = eventTime - (eventTime % windowSizeMs);
        long windowEnd = windowStart + windowSizeMs;
        long windowDeadline = windowEnd + allowedLatenessMs;

        // Calculate the logical delay in minutes relative to the event-time timeline
        double latenessMinutes = (watermark - windowDeadline) / 60000.0;

        if (latenessMinutes > 0) {
            long currentLateness = (long) latenessMinutes;

            // Increment the aggregate cumulative counter
            totalLateRecordsCounter.inc();

            // Atomically update the peak historical lateness if the current boundary is exceeded
            if (currentLateness > maxLatenessMinutes) {
                maxLatenessMinutes = currentLateness;
            }
        }

        // Pass the record through to preserve pipeline throughput
        out.collect(record);
    }
}