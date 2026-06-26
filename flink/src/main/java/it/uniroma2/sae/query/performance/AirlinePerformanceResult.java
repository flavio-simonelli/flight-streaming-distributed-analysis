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
 */
public class AirlinePerformanceResult implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @JsonProperty("window_start")
    private final String windowStart;

    @JsonProperty("window_end")
    private final String windowEnd;

    @JsonProperty("airline")
    private final String airline;

    @JsonProperty("num_flights")
    private final long numFlights;

    @JsonProperty("completed")
    private final long completed;

    @JsonProperty("cancelled")
    private final long cancelled;

    @JsonProperty("diverted")
    private final long diverted;

    @JsonProperty("dep_delay_mean")
    private final double depDelayMean;

    @JsonProperty("cancellation_rate")
    private final double cancellationRate;

    @JsonProperty("late_departure_rate")
    private final double lateDepartureRate;

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
     */
    public static class AirlinePerformanceRecordSerializer implements KafkaRecordSerializationSchema<AirlinePerformanceResult> {
        private static final Logger LOG = LoggerFactory.getLogger(AirlinePerformanceRecordSerializer.class);

        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        private static final ObjectMapper objectMapper = new ObjectMapper();

        public AirlinePerformanceRecordSerializer(KafkaConfig config) {
            this.topic = config.getOutputTopic("q1");
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                AirlinePerformanceResult element, KafkaSinkContext ctx, Long timestamp) {

            byte[] key = element.getAirline() != null ? element.getAirline().getBytes(StandardCharsets.UTF_8) : null;
            byte[] value = null;
            try {
                value = objectMapper.writeValueAsBytes(element);
            } catch (Exception e) {
                LOG.warn("Failed to serialize AirlinePerformanceResult", e);
            }

            return new ProducerRecord<>(topic, key, value);
        }
    }
}
