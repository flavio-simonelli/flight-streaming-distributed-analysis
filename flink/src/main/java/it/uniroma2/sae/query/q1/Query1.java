package it.uniroma2.sae.query.q1;

import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;

/**
 * Query 1 pipeline assembler.
 *
 * Builds the Flink sub-DAG for Query 1 starting from the shared FlightRecord stream.
 * Follows the Builder pattern: construction is encapsulated here and the caller
 * only invokes build(stream), keeping FlightAnalysisJob clean of query logic.
 *
 * Pipeline:
 *   mainStream
 *     -> filter (AA, DL, UA, WN)
 *     -> keyBy(airline)
 *     -> TumblingEventTimeWindow(1h)
 *     -> aggregate(Q1Aggregator, Q1WindowProcessor)
 */
public class Query1 {

    private static final Set<String> TARGET_AIRLINES = Set.of("AA", "DL", "UA", "WN");
    private static final Duration WINDOW_SIZE = Duration.ofHours(1);

    /**
     * Attaches the Query 1 pipeline to the provided shared stream.
     *
     * @param mainStream shared, watermarked DataStream of clean FlightRecord events
     */
    public DataStream<String> build(DataStream<FlightRecord> mainStream) {

        return mainStream
                .filter(event -> TARGET_AIRLINES.contains(event.getAirline()))
                .name("Q1: Filter Airlines")
                .keyBy(FlightRecord::getAirline)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .aggregate(new Q1Aggregator(), new Q1WindowProcessor())
                .name("Q1: Aggregate (1h Tumbling Window)");
    }

    /**
     * Serializer: encodes a JSON string result as a Kafka message without a business key
     * (round-robin distribution across partitions, appropriate for the low-volume Q1 output).
     */
    public static class Q1RecordSerializer implements KafkaRecordSerializationSchema<String>, Serializable {

        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        public Q1RecordSerializer(KafkaConfig config) {
            this.topic = config.getOutputTopic("q1");
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                String element, KafkaSinkContext ctx, Long timestamp) {
            return new ProducerRecord<>(
                    topic, null, null, null,
                    element.getBytes(StandardCharsets.UTF_8));
        }
    }
}
