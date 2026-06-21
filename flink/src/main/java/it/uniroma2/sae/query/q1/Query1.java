package it.uniroma2.sae.query.q1;

import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
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
     * Attaches the Query 1 pipeline and its Kafka sink to the provided shared stream infrastructure.
     * Uses explicit implementations instead of anonymous inline expressions to maintain cluster execution stability.
     *
     * @param mainStream shared, watermarked DataStream of clean FlightRecord events
     * @param kafkaConfig configuration details for building the target Kafka cluster output endpoints
     * @return the resulting data stream topology terminal linkage reference
     */
    public static DataStreamSink<String> buildAndAttach(DataStream<FlightRecord> mainStream, KafkaConfig kafkaConfig) {

        DataStream<String> q1Stream = mainStream
                .filter(new AirlineFilter())
                .startNewChain()
                .name("Q1: Filter Airlines")
                .keyBy(FlightRecord::getAirline)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .allowedLateness(Duration.ofMinutes(5))
                .aggregate(new Q1Aggregator(), new Q1WindowProcessor())
                .name("Q1: Aggregate (1h Tumbling Window)");

        KafkaSink<String> sink = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new Q1RecordSerializer(kafkaConfig))
                .build();

        return q1Stream.sinkTo(sink)
                .name("Q1 Sink: -> " + kafkaConfig.getOutputTopic("q1"));
    }

    /**
     * Dedicated FilterFunction implementation isolating targeting constraints.
     * Declared as an explicit static class to bypass cluster deployment runtime lambda serialization defects.
     */
    public static class AirlineFilter implements FilterFunction<FlightRecord> {

        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(FlightRecord event) throws Exception {
            return event != null && TARGET_AIRLINES.contains(event.getAirline());
        }
    }

    /**
     * Serializer: encodes a JSON string result as a Kafka message without a business key.
     * Uses round-robin distribution across partitions, appropriate for the low-volume Q1 output metadata.
     */
    public static class Q1RecordSerializer implements KafkaRecordSerializationSchema<String> {

        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        /**
         * Standard configuration routing constructor instance setup.
         *
         * @param config application runtime target profile configurations
         */
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
