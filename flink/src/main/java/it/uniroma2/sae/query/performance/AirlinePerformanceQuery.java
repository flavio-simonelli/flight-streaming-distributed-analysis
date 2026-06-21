package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.query.common.BaseAirlineQuery;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Query 1 logic: Airline operational performance metrics.
 * Runs on 1-hour tumbling windows over target carrier events.
 */
public class AirlinePerformanceQuery extends BaseAirlineQuery {
    private static final Duration WINDOW_SIZE = Duration.ofHours(1);

    /**
     * Attaches the AirlinePerformanceQuery pipeline to the pre-filtered target airlines stream.
     */
    public static DataStreamSink<String> buildAndAttach(DataStream<FlightRecord> targetAirlinesStream, KafkaConfig kafkaConfig) {
        DataStream<String> stream = targetAirlinesStream
                .keyBy(FlightRecord::getAirline)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .allowedLateness(Duration.ofMinutes(5))
                .aggregate(new AirlinePerformanceAggregator(), new AirlinePerformanceWindowProcessor())
                .name("Q1: Performance (1h)");

        KafkaSink<String> sink = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new AirlinePerformanceRecordSerializer(kafkaConfig))
                .build();

        return stream.sinkTo(sink)
                .name("Q1: Sink");
    }

    /**
     * Serializer for sending AirlinePerformance JSON strings to Kafka.
     */
    public static class AirlinePerformanceRecordSerializer implements KafkaRecordSerializationSchema<String> {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        public AirlinePerformanceRecordSerializer(KafkaConfig config) {
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
