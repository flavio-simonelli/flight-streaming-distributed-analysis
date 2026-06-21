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
                .startNewChain();
    }

    /**
     * Filter function that discards anomalous records based on business logic rules.
     */
    public static class LogicalInconsistencyFilter implements FilterFunction<FlightRecord> {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(FlightRecord raw) {
            // Mandatory key fields check: YEAR, MONTH, DAY_OF_MONTH, OP_UNIQUE_CARRIER, CRS_DEP_TIME
            if (raw.getYear() == null || raw.getMonth() == null || raw.getDayOfMonth() == null 
                    || raw.getAirline() == null || raw.getAirline().trim().isEmpty() 
                    || raw.getCrsDepTime() == null) {
                return false;
            }

            // State logic inconsistencies:
            // A flight cannot be both cancelled and diverted
            if (raw.isCancelled() && raw.isDiverted()) {
                return false;
            }

            return true;
        }
    }
}
