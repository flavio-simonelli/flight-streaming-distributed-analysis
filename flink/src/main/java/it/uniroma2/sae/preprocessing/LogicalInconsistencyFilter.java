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
     * Evaluates the schema integrity and aviation business logic of a record.
     * Evaluates four levels of consistency checks sequentially. If any check fails, the record
     * is dropped from the stream and the corrupted records counter metric is incremented.
     *
     * @param raw the raw FlightRecord to evaluate
     * @return true if the record passes all validation layers, false if it should be discarded
     */
    private boolean filterInternal(FlightRecord raw) {
        // --- RULE 1: Object Null Safety Check ---
        // Safeguards the downstream pipeline against NullPointerExceptions by discarding null references.
        if (raw == null) {
            incrementCorruptedCounter();
            return false;
        }

        // --- RULE 2: Schema Completeness Check ---
        // Verifies that all fields required for stream routing (keyBy) and windowing are present.
        // Missing fields in:
        // - Year, Month, Day, or Departure Time: Breaks Event-Time calculation (Watermarking).
        // - Airline: Breaks Query 1 and Query 3 keyBy partitioning.
        // - Airport IDs: Breaks Query 2 keyBy partitioning.
        if (raw.getYear() == null || raw.getMonth() == null || raw.getDayOfMonth() == null
                || raw.getAirline() == null || raw.getAirline().trim().isEmpty()
                || raw.getCrsDepTime() == null
                || raw.getOriginAirportId() == null || raw.getDestinationAirportId() == null
        ) {
            incrementCorruptedCounter();
            return false;
        }

        // --- RULE 3: Temporal Parse Validity Check ---
        // Ensures that the event time was successfully calculated during Kafka deserialization.
        // If 'calculateEpochMillis' failed, 'eventTimeMillis' is set to Long.MIN_VALUE.
        // An event time of Long.MIN_VALUE or a negative departure hour would stall Flink's
        // event-time watermark progression, causing state memory leaks and halting window firings.
        if (raw.getEventTimeMillis() == Long.MIN_VALUE || raw.getScheduledDepartureHour() < 0) {
            incrementCorruptedCounter();
            return false;
        }

        // --- RULE 4: Logical Business Consistency Check ---
        // In aviation, a flight cannot be both cancelled (never took off) and diverted (took off but rerouted).
        // Having both flags set to true (value of 1.0) represents corrupted record states.
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
