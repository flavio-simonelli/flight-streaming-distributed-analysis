package it.uniroma2.sae.query.performance;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sae.config.KafkaConfig;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Output record representing operational performance metrics for a carrier per tumbling window.
 * Maps data objects structurally to match the JSON/CSV serialization layouts for Query 1.
 */
public class AirlinePerformanceResult implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** Human-readable formatted string representing the inclusive starting bounds of the time window. */
    @JsonProperty("window_start")
    private final String windowStart;

    /** Human-readable formatted string representing the exclusive ending bounds of the time window. */
    @JsonProperty("window_end")
    private final String windowEnd;

    /** The unique carrier code identifying the airline under analysis. */
    @JsonProperty("airline")
    private final String airline;

    /** The grand total of all tracked flight events within this window. */
    @JsonProperty("num_flights")
    private final long numFlights;

    /** The count of flights that successfully arrived at their destination. */
    @JsonProperty("completed")
    private final long completed;

    /** The count of scheduled flights that were canceled. */
    @JsonProperty("cancelled")
    private final long cancelled;

    /** The count of flights that were rerouted to a different airport. */
    @JsonProperty("diverted")
    private final long diverted;

    /** The calculated mathematical mean of departure delays across all operated flights. */
    @JsonProperty("dep_delay_mean")
    private final double depDelayMean;

    /** The percentage of flights canceled relative to total observed operations. */
    @JsonProperty("cancellation_rate")
    private final double cancellationRate;

    /** The percentage of operated flights that exceeded the 15-minute delay limit. */
    @JsonProperty("late_departure_rate")
    private final double lateDepartureRate;

    /**
     * Fully parameterized constructor to instantiate an immutable query 1 result snapshot.
     *
     * @param windowStart the formatted window start timestamp
     * @param windowEnd the formatted window end timestamp
     * @param airline the unique carrier designator string
     * @param numFlights total flight tracking count
     * @param cancelled total cancellation occurrences
     * @param diverted total diversion occurrences
     * @param completed total successfully completed flights
     * @param depDelayMean average departure delay minutes
     * @param cancellationRate percentage tracking cancellation frequency
     * @param lateDepartureRate percentage tracking severe departure delay occurrences
     */
    public AirlinePerformanceResult(
            String windowStart, String windowEnd, String airline,
            long numFlights, long cancelled, long diverted, long completed,
            double depDelayMean,
            double cancellationRate, double lateDepartureRate) {
        this.windowStart       = windowStart;
        this.windowEnd         = windowEnd;
        this.airline           = airline;
        this.numFlights        = numFlights;
        this.cancelled         = cancelled;
        this.diverted          = diverted;
        this.completed         = completed;
        this.depDelayMean      = depDelayMean;
        this.cancellationRate  = cancellationRate;
        this.lateDepartureRate = lateDepartureRate;
    }

    // --- Getters ---

    public String getWindowStart()    { return windowStart; }
    public String getWindowEnd()      { return windowEnd; }
    public String getAirline()        { return airline; }
    public long getNumFlights()       { return numFlights; }
    public long getCancelled()        { return cancelled; }
    public long getDiverted()         { return diverted; }
    public long getCompleted()        { return completed; }
    public double getDepDelayMean()   { return depDelayMean; }
    public double getCancellationRate()  { return cancellationRate; }
    public double getLateDepartureRate() { return lateDepartureRate; }

    /**
     * Generates a string snapshot of the performance metrics entry for logging and tracking.
     *
     * @return a text representation listing calculated values
     */
    @Override
    public String toString() {
        return "AirlinePerformanceOutput{" +
                "airline='" + airline + '\'' +
                ", window=[" + windowStart + "," + windowEnd + "]" +
                ", numFlights=" + numFlights +
                ", cancelled=" + cancelled +
                ", diverted=" + diverted +
                ", depDelayMean=" + depDelayMean +
                ", cancellationRate=" + cancellationRate +
                ", lateDepartureRate=" + lateDepartureRate +
                '}';
    }

    /**
     * Serializer for sending AirlinePerformanceResult to Kafka as JSON bytes.
     * Integrates with Flink's KafkaSink architecture to stream structural metrics.
     */
    public static class AirlinePerformanceRecordSerializer implements KafkaRecordSerializationSchema<AirlinePerformanceResult> {
        private static final Logger LOG = LoggerFactory.getLogger(AirlinePerformanceRecordSerializer.class);

        /** The serial version UID for schema class validation. */
        @Serial
        private static final long serialVersionUID = 1L;

        /** The targeted destination Kafka topic name resolved for Query 1 output. */
        private final String topic;

        /** Shared thread-safe Jackson ObjectMapper instance for translating POJOs into JSON data chunks. */
        private static final ObjectMapper objectMapper = new ObjectMapper();

        /**
         * Contextual constructor extracting the appropriate routing topic via application settings.
         *
         * @param config the container defining Kafka environment profiles
         */
        public AirlinePerformanceRecordSerializer(KafkaConfig config) {
            this.topic = config.getOutputTopic("q1");
        }

        /**
         * Transforms the structured result entity into a low-level key-value Kafka ProducerRecord.
         *
         * @param element the computed metric entity arriving from the window processor
         * @param ctx runtime sink context providing metadata helpers
         * @param timestamp the internal timestamp mapping attached to the stream entity
         * @return a serialized Kafka producer record wrapper ready for network I/O delivery
         */
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                AirlinePerformanceResult element, KafkaSinkContext ctx, Long timestamp) {

            // Extract the airline code to use as the message routing key for Kafka partition balancing
            byte[] key   = null;
            byte[] value = null;

            try {
                // Convert the whole entity state to a raw UTF-8 JSON byte array payload
                key = element.getAirline().getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                // Enforce error fault tolerance by catching serialization structural flaws gracefully
                LOG.warn("Failed to serialize airline", e);
            }

            try {
                // Convert the whole entity state to a raw UTF-8 JSON byte array payload
                value = objectMapper.writeValueAsBytes(element);
            } catch (Exception e) {
                // Enforce error fault tolerance by catching serialization structural flaws gracefully
                LOG.warn("Failed to serialize AirlinePerformanceResult", e);
            }

            // Return the compiled record pointing to the target output topic destination
            return new ProducerRecord<>(topic, key, value);
        }
    }
}