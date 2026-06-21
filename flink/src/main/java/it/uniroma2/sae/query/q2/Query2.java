package it.uniroma2.sae.query.q2;

import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * Query 2 pipeline assembler.
 * Identifies origin airports with significant delays across multiple time horizons.
 * Combines tumbling windows with global state analytics to output ranked latency statistics.
 */
public class Query2 {

    /**
     * Builds and attaches the Query 2 execution sub-DAG to the shared flight stream.
     * Evaluates metrics concurrently over one-hour, six-hour, and global historic horizons.
     *
     * @param mainStream the shared, watermarked data stream of verified FlightRecord events
     * @param kafkaConfig configuration parameters for cluster routing and output topics
     * @return a list containing the finalized sink endpoints for data orchestration management
     */
    public static List<DataStreamSink<String>> buildAndAttach(DataStream<FlightRecord> mainStream, KafkaConfig kafkaConfig) {

        KeyedStream<FlightRecord, Integer> keyedStream = mainStream.keyBy(FlightRecord::getOriginAirportId);

        // TODO: fine tuning
        DataStream<String> w1 = createTumblingWindowPipeline(keyedStream, Duration.ofHours(1), "1h");
        DataStream<String> w6 = createTumblingWindowPipeline(keyedStream, Duration.ofHours(6), "6h");

        DataStream<String> wGlobal = keyedStream
                .process(new Q2GlobalStateProcessor())
                .name("Q2: Global Accumulation")
                .keyBy(Q2AirportResult::getWindowEnd)
                .process(new Q2RankProcessor())
                .name("Q2: Global Rank");

        return List.of(
                attachKafkaSink(w1, kafkaConfig, "q2_1h", "Q2 1h"),
                attachKafkaSink(w6, kafkaConfig, "q2_6h", "Q2 6h"),
                attachKafkaSink(wGlobal, kafkaConfig, "q2_global", "Q2 Global")
        );
    }

    /**
     * Constructs a tumbling event-time window pipeline architecture.
     * Computes localized metrics per window and instantly evaluates ranking without redundant state allocation.
     *
     * @param keyedStream the primary flight stream already partitioned by origin airport identifiers
     * @param windowSize the temporal tracking duration of the tumbling window definition
     * @param label descriptive suffix applied directly to downstream operator names
     * @return a data stream of formatted string values ready for sink persistence
     */
    private static DataStream<String> createTumblingWindowPipeline(
            KeyedStream<FlightRecord, Integer> keyedStream,
            Duration windowSize,
            String label) {

        return keyedStream
                .window(TumblingEventTimeWindows.of(windowSize))
                .aggregate(new Q2Aggregator(), new Q2WindowProcessor())
                .name("Q2: Aggregate (" + label + " Tumbling Window)")
                .keyBy(Q2AirportResult::getWindowEnd)
                .process(new Q2RankProcessor())
                .name("Q2: " + label + " Rank");
    }

    /**
     * Instantiates and connects an isolated Apache Kafka sink consumer step.
     * Encourages single-responsibility metrics mapping while abstracting boilerplate pipeline wiring.
     *
     * @param stream the data stream releasing parsed analytical payload strings
     * @param kafkaConfig connection metadata parameters enclosing targets and server registries
     * @param queryKey internal routing key lookup indicator targeting the structural topic definition
     * @param pipelineName verbose name tag used for execution tracking representation within the cluster
     * @return the resulting data stream topology terminal linkage reference
     */
    private static DataStreamSink<String> attachKafkaSink(
            DataStream<String> stream,
            KafkaConfig kafkaConfig,
            String queryKey,
            String pipelineName) {

        KafkaSink<String> sink = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new Q2RecordSerializer(kafkaConfig, queryKey))
                .build();

        return stream.sinkTo(sink)
                .name(pipelineName + ": Kafka Sink -> " + kafkaConfig.getOutputTopic(queryKey));
    }

    /**
     * Serializer: encodes the result as a Kafka message, routing it to partition
     * based on ORIGIN_AIRPORT_ID to guarantee ordering.
     */
    public static class Q2RecordSerializer implements KafkaRecordSerializationSchema<String> {

        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        /**
         * Contextual constructor defining the targeting topology configurations.
         *
         * @param config the structural cluster routing references
         * @param queryKey mapping target lookup context parameter identifier
         */
        public Q2RecordSerializer(KafkaConfig config, String queryKey) {
            this.topic = config.getOutputTopic(queryKey);
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                String element, KafkaSinkContext ctx, Long timestamp) {

            byte[] key = null;
            try {
                String[] fields = element.split(",");
                if (fields.length > 2) {
                    String originAirportId = fields[2].trim();
                    key = originAirportId.getBytes(StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                // Fallback to null key if parsing fails
            }

            return new ProducerRecord<>(
                    topic, null, null, key,
                    element.getBytes(StandardCharsets.UTF_8));
        }
    }
}