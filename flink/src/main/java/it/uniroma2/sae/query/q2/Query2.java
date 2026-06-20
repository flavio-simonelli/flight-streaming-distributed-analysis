package it.uniroma2.sae.query.q2;

import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Query 2 pipeline assembler.
 * Identifies origin airports with significant delays.
 */
public class Query2 {

    public void buildAndAttach(DataStream<FlightRecord> mainStream, KafkaConfig kafkaConfig) {
        
        // Optimize network transfer: key the raw stream once and reuse it across all pipelines
        // to avoid redundant network shuffles (network serialization/deserialization) of the raw events.
        KeyedStream<FlightRecord, Integer> keyedStream = mainStream.keyBy(FlightRecord::getOriginAirportId);

        // 1. 1 Hour event-time tumbling window pipeline
        DataStream<String> w1 = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new Q2Aggregator(), new Q2WindowProcessor())
                .name("Q2: Aggregate (1h Tumbling Window)")
                .keyBy(Q2AirportResult::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .process(new Q2RankProcessor())
                .name("Q2: 1h Rank");

        // 2. 6 Hours event-time tumbling window pipeline
        DataStream<String> w6 = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .aggregate(new Q2Aggregator(), new Q2WindowProcessor())
                .name("Q2: Aggregate (6h Tumbling Window)")
                .keyBy(Q2AirportResult::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .process(new Q2RankProcessor())
                .name("Q2: 6h Rank");

        // 3. Global window (cumulative) pipeline, updated hourly
        DataStream<String> wGlobal = keyedStream
                .process(new Q2GlobalStateProcessor())
                .name("Q2: Global Accumulation")
                .keyBy(Q2AirportResult::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .process(new Q2RankProcessor())
                .name("Q2: Global Rank");

        // Sinks
        KafkaSink<String> sink2_1h = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new Q2RecordSerializer(kafkaConfig, "q2_1h"))
                .build();

        KafkaSink<String> sink2_6h = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new Q2RecordSerializer(kafkaConfig, "q2_6h"))
                .build();

        KafkaSink<String> sink2_global = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new Q2RecordSerializer(kafkaConfig, "q2_global"))
                .build();

        w1.sinkTo(sink2_1h)
                .name("Q2 1h: Kafka Sink -> " + kafkaConfig.getOutputTopic("q2_1h"));

        w6.sinkTo(sink2_6h)
                .name("Q2 6h: Kafka Sink -> " + kafkaConfig.getOutputTopic("q2_6h"));

        wGlobal.sinkTo(sink2_global)
                .name("Q2 Global: Kafka Sink -> " + kafkaConfig.getOutputTopic("q2_global"));
    }

    /**
     * Serializer: encodes the result as a Kafka message, routing it to partition
     * based on ORIGIN_AIRPORT_ID to guarantee ordering.
     */
    public static class Q2RecordSerializer implements KafkaRecordSerializationSchema<String> {

        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

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
