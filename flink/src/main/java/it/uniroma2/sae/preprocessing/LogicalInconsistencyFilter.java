package it.uniroma2.sae.preprocessing;

import it.uniroma2.sae.metrics.ProcessingLatencyTracker;
import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.metrics.Counter;

import java.io.Serial;

/**
 * Filter function that discards anomalous records based on strict business logic rules.
 * Registers custom Flink metrics to track corrupted records and processing latency.
 */
public class LogicalInconsistencyFilter extends RichFilterFunction<FlightRecord> {

    @Serial
    private static final long serialVersionUID = 1L;

    private transient Counter corruptedRecordsCounter;
    private transient ProcessingLatencyTracker latencyTracker;

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        this.corruptedRecordsCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("corrupted_records_total");
        this.latencyTracker = new ProcessingLatencyTracker("preprocessing");
        this.latencyTracker.register(getRuntimeContext().getMetricGroup());
    }

    /**
     * Evaluates each flight record against strict data quality and business consistency rules.
     *
     * @param raw the incoming flight record to evaluate
     * @return true if the record meets all consistency criteria, false if it should be dropped
     */
    @Override
    public boolean filter(FlightRecord raw) {
        long start = System.nanoTime();
        boolean res = filterInternal(raw);
        double duration = (System.nanoTime() - start) / 1_000_000.0;

        if (latencyTracker != null) {
            latencyTracker.updateOperator(duration);
            if (raw != null) {
                latencyTracker.updateE2E(raw.getSystemIngestionTime());
            }
        }
        return res;
    }

    /**
     * Internal filtering logic evaluating schema completeness and logical rules.
     */
    private boolean filterInternal(FlightRecord raw) {
        // Drop null references safely to protect downstream operators from NullPointerExceptions
        if (raw == null) {
            incrementCorruptedCounter();
            return false;
        }

        // Mandatory key fields check: YEAR, MONTH, DAY_OF_MONTH, OP_UNIQUE_CARRIER, CRS_DEP_TIME, and IDs.
        // Verifies that all vital fields required for partitioning and windowing are present.
        if (raw.getYear() == null || raw.getMonth() == null || raw.getDayOfMonth() == null
                || raw.getAirline() == null || raw.getAirline().trim().isEmpty()
                || raw.getCrsDepTime() == null
                || raw.getOriginAirportId() == null || raw.getDestinationAirportId() == null
        ) {
            incrementCorruptedCounter();
            return false;
        }

        // Validate that the timestamp parsing was successful during Kafka ingestion.
        // If calculateEpochMillis failed, eventTimeMillis will be Long.MIN_VALUE, breaking the watermark.
        if (raw.getEventTimeMillis() == Long.MIN_VALUE || raw.getScheduledDepartureHour() < 0) {
            incrementCorruptedCounter();
            return false;
        }

        // State logic inconsistencies check:
        // A flight cannot be both canceled and diverted simultaneously under real-world aviation rules.
        if (raw.isCancelled() && raw.isDiverted()) {
            incrementCorruptedCounter();
            return false;
        }

        return true;
    }

    private void incrementCorruptedCounter() {
        if (corruptedRecordsCounter != null) {
            corruptedRecordsCounter.inc();
        }
    }
}
