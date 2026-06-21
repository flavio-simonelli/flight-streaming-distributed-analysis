package it.uniroma2.sae.query.rank;

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
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * Query 2 logic: Classifies top 10 origin airports with severe delays.
 * Runs concurrently over 1h, 6h tumbling windows, and global state.
 */
public class RankAirportsQuery implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Builds and attaches the RankAirportsQuery pipeline to the main preprocessed stream.
     */
    public static List<DataStreamSink<String>> buildAndAttach(DataStream<FlightRecord> mainStream, KafkaConfig kafkaConfig) {
        KeyedStream<FlightRecord, Integer> keyedStream = mainStream.keyBy(FlightRecord::getOriginAirportId);

        DataStream<String> w1 = createTumblingWindowPipeline(keyedStream, Duration.ofHours(1), "1h");
        DataStream<String> w6 = createTumblingWindowPipeline(keyedStream, Duration.ofHours(6), "6h");

        DataStream<String> wGlobal = keyedStream
                .process(new RankAirportsGlobalStateProcessor())
                .name("Q2: Global Accum")
                .keyBy(RankAirportsResult::getWindowEnd)
                .process(new RankAirportsRankProcessor())
                .name("Q2: Global Rank");

        return List.of(
                attachKafkaSink(w1, kafkaConfig, "q2_1h", "Q2: 1h"),
                attachKafkaSink(w6, kafkaConfig, "q2_6h", "Q2: 6h"),
                attachKafkaSink(wGlobal, kafkaConfig, "q2_global", "Q2: Global")
        );
    }

    private static DataStream<String> createTumblingWindowPipeline(
            KeyedStream<FlightRecord, Integer> keyedStream,
            Duration windowSize,
            String label) {

        return keyedStream
                .window(TumblingEventTimeWindows.of(windowSize))
                .aggregate(new RankAirportsAggregator(), new RankAirportsWindowProcessor(label))
                .name("Q2: Window (" + label + ")")
                .keyBy(RankAirportsResult::getWindowEnd)
                .process(new RankAirportsRankProcessor())
                .name("Q2: Rank (" + label + ")");
    }

    private static DataStreamSink<String> attachKafkaSink(
            DataStream<String> stream,
            KafkaConfig kafkaConfig,
            String queryKey,
            String pipelineName) {

        KafkaSink<String> sink = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new RankAirportsRecordSerializer(kafkaConfig, queryKey))
                .build();

        return stream.sinkTo(sink)
                .name(pipelineName + " Sink");
    }

    /**
     * Serializer for sending RankAirports JSON strings to Kafka.
     * Roots message keys by origin_airport_id for ordered delivery.
     */
    public static class RankAirportsRecordSerializer implements KafkaRecordSerializationSchema<String> {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        public RankAirportsRecordSerializer(KafkaConfig config, String queryKey) {
            this.topic = config.getOutputTopic(queryKey);
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                String element, KafkaSinkContext ctx, Long timestamp) {

            byte[] key = null;
            try {
                // Parse origin_airport_id from the JSON string if needed,
                // or just route without key or parse manually.
                // Since it is a JSON string, we can do a simple substring or regex, or Jackson.
                // Let's do a fast manual JSON field extract: "origin_airport_id":12345
                int idx = element.indexOf("\"origin_airport_id\":");
                if (idx != -1) {
                    int start = idx + "\"origin_airport_id\":".length();
                    int end = start;
                    while (end < element.length() && Character.isDigit(element.charAt(end))) {
                        end++;
                    }
                    String airportId = element.substring(start, end);
                    key = airportId.getBytes(StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                // Fallback
            }

            return new ProducerRecord<>(
                    topic, null, null, key,
                    element.getBytes(StandardCharsets.UTF_8));
        }
    }
}
