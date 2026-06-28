package it.uniroma2.sae.metrics;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * A custom {@link ProcessFunction} designed to analyze, track, and report the event-time
 * lateness distribution of delayed {@link FlightRecord} events.
 * <p>
 * This version implements {@link CheckpointedFunction} to ensure that the aggregate historical
 * peak lateness metric is saved within Flink's managed operator state, making it fault-tolerant
 * and preserving its value across cluster crashes and restarts.
 * </p>
 */
public class LateRecordMetricAnalyzer extends ProcessFunction<FlightRecord, FlightRecord>
        implements CheckpointedFunction, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** The size of the tumbling event-time window expressed in milliseconds. */
    private final long windowSizeMs;

    /** The allowed lateness buffer configuration expressed in milliseconds. */
    private final long allowedLatenessMs;

    /** Flink metric counter used to accumulate the total number of late records encountered. */
    private transient Counter totalLateRecordsCounter;

    /** In-memory tracking variable for the peak historical lateness. */
    private transient long maxLatenessMinutes = 0;

    /** Managed Flink operator state used to persist the maximum lateness across checkpoints. */
    private transient ListState<Long> checkpointedMaxLatenessState;

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

        // Register the Gauge using the local variable synchronized via state recovery
        getRuntimeContext()
                .getMetricGroup()
                .gauge("max_lateness_minutes_absolute", () -> maxLatenessMinutes);
    }

    /**
     * Processes each late-arriving flight record, evaluating its event-time delay against the window deadline.
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

        long watermark = ctx.timerService().currentWatermark();
        long windowStart = eventTime - (eventTime % windowSizeMs);
        long windowEnd = windowStart + windowSizeMs;
        long windowDeadline = windowEnd + allowedLatenessMs;

        double latenessMinutes = (watermark - windowDeadline) / 60000.0;

        if (latenessMinutes > 0) {
            long currentLateness = (long) latenessMinutes;

            totalLateRecordsCounter.inc();

            if (currentLateness > maxLatenessMinutes) {
                maxLatenessMinutes = currentLateness;
            }
        }

        out.collect(record);
    }

    /**
     * Invoked when Flink triggers a snapshot/checkpoint. Synchronizes the local memory
     * variable into Flink's fault-tolerant state backend storage.
     *
     * @param context the context for taking a snapshot
     * @throws Exception if state appending or clearing fails
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedMaxLatenessState.clear();
        checkpointedMaxLatenessState.add(maxLatenessMinutes);
    }

    /**
     * Invoked during task initialization to restore state from a previous checkpoint or savepoint.
     * Re-populates the local {@code maxLatenessMinutes} variable if a recovery occurs.
     *
     * @param context the context for initializing the operator state
     * @throws Exception if state descriptor registration or unpacking fails
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                "max-lateness-state-store",
                Types.LONG
        );

        this.checkpointedMaxLatenessState = context.getOperatorStateStore().getListState(descriptor);

        // If the job is recovering from a previous failure, restore the maximum metric value
        if (context.isRestored()) {
            for (Long val : checkpointedMaxLatenessState.get()) {
                this.maxLatenessMinutes = val;
            }
        }
    }
}