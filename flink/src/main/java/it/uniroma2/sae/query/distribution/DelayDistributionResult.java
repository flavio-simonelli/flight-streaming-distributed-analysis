package it.uniroma2.sae.query.distribution;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sae.config.KafkaConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
import java.io.Serializable;

/**
 * Result data model holding delay percentiles for a distinct airline and hour.
 * Serialized as JSON with explicit window type identification.
 */
public class DelayDistributionResult implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @JsonProperty("window_start")
    private long windowStart;

    @JsonProperty("window_end")
    private long windowEnd;

    @JsonProperty("window_type")
    private String windowType; // "1d", "7d", "global"

    @JsonProperty("airline")
    private String airline;

    @JsonProperty("hour")
    private int hour;

    @JsonProperty("count")
    private long count;

    @JsonProperty("min")
    private double min;

    @JsonProperty("p25")
    private double p25;

    @JsonProperty("p50")
    private double p50;

    @JsonProperty("p75")
    private double p75;

    @JsonProperty("p90")
    private double p90;

    @JsonProperty("max")
    private double max;

    static DelayDistributionResult fromAccumulator(
            long windowStart,
            long windowEnd,
            String windowType,
            Tuple2<String, Integer> key,
            DelayDistributionAccumulator acc) {

        return new DelayDistributionResult(
                windowStart,
                windowEnd,
                windowType,
                key.f0,
                key.f1,
                acc.getCount(),
                acc.getMin(),
                acc.getPercentile(0.25),
                acc.getPercentile(0.50),
                acc.getPercentile(0.75),
                acc.getPercentile(0.90),
                acc.getMax()
        );
    }

    public DelayDistributionResult(
            long windowStart, long windowEnd, String windowType,
            String airline, int hour, long count,
            double min, double p25, double p50, double p75, double p90, double max) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.windowType = windowType;
        this.airline = airline;
        this.hour = hour;
        this.count = count;
        this.min = min;
        this.p25 = p25;
        this.p50 = p50;
        this.p75 = p75;
        this.p90 = p90;
        this.max = max;
    }

    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public String getWindowType() { return windowType; }
    public void setWindowType(String windowType) { this.windowType = windowType; }

    public String getAirline() { return airline; }
    public void setAirline(String airline) { this.airline = airline; }

    public int getHour() { return hour; }
    public void setHour(int hour) { this.hour = hour; }

    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }

    public double getMin() { return min; }
    public void setMin(double min) { this.min = min; }

    public double getP25() { return p25; }
    public void setP25(double p25) { this.p25 = p25; }

    public double getP50() { return p50; }
    public void setP50(double p50) { this.p50 = p50; }

    public double getP75() { return p75; }
    public void setP75(double p75) { this.p75 = p75; }

    public double getP90() { return p90; }
    public void setP90(double p90) { this.p90 = p90; }

    public double getMax() { return max; }
    public void setMax(double max) { this.max = max; }

    /**
     * Serializer for sending DelayDistributionResult objects to Kafka topics in standard JSON formats.
     */
    public static class DelayDistributionRecordSerializer implements KafkaRecordSerializationSchema<DelayDistributionResult> {

        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        /** Shared Jackson Object Mapper instance for efficient JSON serialization. */
        private static final ObjectMapper MAPPER = new ObjectMapper();

        /**
         * Creates a new serializer mapped to a precise target storage topic.
         *
         * @param config the central Kafka topology properties instance
         * @param queryKey the internal key matching the destination output configuration topic
         */
        public DelayDistributionRecordSerializer(KafkaConfig config, String queryKey) {
            this.topic = config.getOutputTopic(queryKey);
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                DelayDistributionResult element, KafkaSinkContext ctx, Long timestamp) {
            byte[] value = null;
            try {
                value = MAPPER.writeValueAsBytes(element);
            } catch (Exception e) {
                // Defensive fallback serialization handler
            }
            return new ProducerRecord<>(topic, null, value);
        }
    }
}
