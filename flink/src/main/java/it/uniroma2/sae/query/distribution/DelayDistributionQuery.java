package it.uniroma2.sae.query.distribution;

import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.query.common.BaseAirlineQuery;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * Query 3 logic: Departure delay distribution (percentiles) per airline and hourly slot.
 * Runs concurrently over 1d, 7d tumbling windows, and global state.
 */
public class DelayDistributionQuery extends BaseAirlineQuery {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Builds and attaches the DelayDistributionQuery pipelines to the shared target airlines stream.
     */
    public static List<DataStreamSink<String>> buildAndAttach(DataStream<FlightRecord> targetAirlinesStream, KafkaConfig kafkaConfig) {
        
        // Key by airline and hour (0-23)
        KeyedStream<FlightRecord, Tuple2<String, Integer>> keyedStream = targetAirlinesStream
                .keyBy(
                        event -> Tuple2.of(event.getAirline(), event.getCrsDepTime() / 100),
                        TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Integer>>() {})
                );

        DataStream<String> w1d = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionWindowProcessor("1d"))
                .name("Q3: Window (1d)")
                .map(res -> MAPPER.writeValueAsString(res))
                .name("JSON");

        DataStream<String> w7d = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofDays(7)))
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionWindowProcessor("7d"))
                .name("Q3: Window (7d)")
                .map(res -> MAPPER.writeValueAsString(res))
                .name("JSON");

        DataStream<String> wGlobal = keyedStream
                .process(new DelayDistributionGlobalStateProcessor())
                .name("Q3: Global Accum")
                .map(res -> MAPPER.writeValueAsString(res))
                .name("JSON");

        return List.of(
                attachKafkaSink(w1d, kafkaConfig, "q3", "Q3: 1d"),
                attachKafkaSink(w7d, kafkaConfig, "q3", "Q3: 7d"),
                attachKafkaSink(wGlobal, kafkaConfig, "q3", "Q3: Global")
        );
    }

    private static DataStreamSink<String> attachKafkaSink(
            DataStream<String> stream,
            KafkaConfig kafkaConfig,
            String queryKey,
            String pipelineName) {

        KafkaSink<String> sink = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new DelayDistributionRecordSerializer(kafkaConfig, queryKey))
                .build();

        return stream.sinkTo(sink)
                .name(pipelineName + " Sink");
    }

    /**
     * Serializer for sending DelayDistribution JSON strings to Kafka.
     */
    public static class DelayDistributionRecordSerializer implements KafkaRecordSerializationSchema<String> {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        public DelayDistributionRecordSerializer(KafkaConfig config, String queryKey) {
            this.topic = config.getOutputTopic(queryKey);
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
