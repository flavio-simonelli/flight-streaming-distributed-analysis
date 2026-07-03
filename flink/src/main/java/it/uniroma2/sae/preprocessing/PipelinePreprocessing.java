package it.uniroma2.sae.preprocessing;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Preprocessing step that applies data quality filters to clean the incoming flight stream.
 */
public class PipelinePreprocessing {

    /**
     * Applies the preprocessing pipeline on FlightRecord stream by filtering out
     * logically inconsistent or corrupted records.
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
}
