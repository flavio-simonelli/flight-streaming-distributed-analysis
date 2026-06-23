package it.uniroma2.sae.query.distribution;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.query.common.BaseAirlineQuery;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
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
    public static List<DataStreamSink<DelayDistributionResult>> buildAndAttach(DataStream<FlightRecord> targetAirlinesStream, it.uniroma2.sae.config.ApplicationConfig config) {
        KafkaConfig kafkaConfig = config.getKafka();
        Duration allowedLateness = Duration.ofMinutes(config.getFlink().getAllowedLatenessMinutes());

        // Key by airline and hour
        KeyedStream<FlightRecord, Tuple2<String, Integer>> keyedStream = targetAirlinesStream
                .keyBy(
                        event -> Tuple2.of(event.getAirline(), event.getScheduledDepartureHour()),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                );

        DataStream<DelayDistributionResult> w1d = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .allowedLateness(allowedLateness)
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionWindowProcessor("1d"))
                .name("Q3: Window (1d)")
                .uid("q3-window-1d");

        DataStream<DelayDistributionResult> w7d = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofDays(7)))
                .allowedLateness(allowedLateness)
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionWindowProcessor("7d"))
                .name("Q3: Window (7d)")
                .uid("q3-window-7d");

        DataStream<DelayDistributionResult> wGlobal = keyedStream
                .window(org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.create())
                .trigger(org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger.of(Duration.ofDays(1)))
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionGlobalWindowProcessor())
                .name("Q3: Global Window")
                .uid("q3-global-window");

        return List.of(
                attachKafkaSink(w1d, kafkaConfig, "q3", "Q3: 1d", "q3-sink-1d"),
                attachKafkaSink(w7d, kafkaConfig, "q3", "Q3: 7d", "q3-sink-7d"),
                attachKafkaSink(wGlobal, kafkaConfig, "q3", "Q3: Global", "q3-sink-global")
        );
    }

    private static DataStreamSink<DelayDistributionResult> attachKafkaSink(
            DataStream<DelayDistributionResult> stream,
            KafkaConfig kafkaConfig,
            String queryKey,
            String pipelineName,
            String uid) {

        KafkaSink<DelayDistributionResult> sink = new SinkBuilder<DelayDistributionResult>(kafkaConfig)
                .withRecordSerializer(new DelayDistributionRecordSerializer(kafkaConfig, queryKey))
                .build();

        return stream.sinkTo(sink)
                .name(pipelineName + " Sink")
                .uid(uid);
    }

    /**
     * Serializer for sending DelayDistributionResult objects to Kafka.
     */
    public static class DelayDistributionRecordSerializer implements KafkaRecordSerializationSchema<DelayDistributionResult> {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

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
                // Fallback
            }
            return new ProducerRecord<>(
                    topic, null, null, null,
                    value);
        }
    }
}
