package it.uniroma2.sae.preprocessing;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serial;
import java.io.Serializable;

/**
 * Preprocessing and Data Quality step for the Flight Stream pipeline.
 * Performs logical consistency filtering and delay normalization/imputation on FlightRecord.
 */
public class PipelinePreprocessing implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Applies the preprocessing pipeline on FlightRecord stream:
     * - Filters out logically inconsistent or corrupted records.
     *
     * @param rawStream DataStream of deserialized FlightRecord events
     * @return DataStream of preprocessed and clean FlightRecord events
     */
    public static DataStream<FlightRecord> preprocess(DataStream<FlightRecord> rawStream) {
        return rawStream
                .filter(new LogicalInconsistencyFilter())
                .name("Preprocessing Records")
                .uid("preprocessing-filter");
    }

    /**
     * Filter function that discards anomalous records based on business logic rules.
     */
    public static class LogicalInconsistencyFilter extends org.apache.flink.api.common.functions.RichFilterFunction<FlightRecord> {

        @Serial
        private static final long serialVersionUID = 1L;

        private transient org.apache.flink.metrics.Counter corruptedRecordsCounter;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            this.corruptedRecordsCounter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("corrupted_records_total");
        }

        /**
         * Evaluates each flight record against strict data quality and business consistency rules.
         *
         * @param raw the incoming flight record to evaluate
         * @return true if the record meets all consistency criteria, false if it should be dropped
         */
        @Override
        public boolean filter(FlightRecord raw) {
            // Drop null references safely to protect the downstream pipeline from NullPointerExceptions
            if (raw == null) {
                if (corruptedRecordsCounter != null) {
                    corruptedRecordsCounter.inc();
                }
                return false;
            }

            // Mandatory key fields check: YEAR, MONTH, DAY_OF_MONTH, OP_UNIQUE_CARRIER, CRS_DEP_TIME, and IDs
            // Verifies that all vital components required for partitioning and windowing are present
            if (raw.getYear() == null || raw.getMonth() == null || raw.getDayOfMonth() == null
                    || raw.getAirline() == null || raw.getAirline().trim().isEmpty()
                    || raw.getCrsDepTime() == null
                    || raw.getOriginAirportId() == null || raw.getDestinationAirportId() == null
            ) {
                if (corruptedRecordsCounter != null) {
                    corruptedRecordsCounter.inc();
                }
                return false;
            }

            // Validate that the timestamp parsing was successful during Kafka ingestion
            // If calculateEpochMillis failed, eventTimeMillis will be Long.MIN_VALUE, breaking the watermark
            if (raw.getEventTimeMillis() == Long.MIN_VALUE || raw.getScheduledDepartureHour() < 0) {
                if (corruptedRecordsCounter != null) {
                    corruptedRecordsCounter.inc();
                }
                return false;
            }

            // State logic inconsistencies check
            // A flight cannot be both canceled and diverted simultaneously under real-world aviation rules
            if (raw.isCancelled() && raw.isDiverted()) {
                if (corruptedRecordsCounter != null) {
                    corruptedRecordsCounter.inc();
                }
                return false;
            }

            return true;
        }
    }
}
