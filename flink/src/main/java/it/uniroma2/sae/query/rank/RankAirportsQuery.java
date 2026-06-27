package it.uniroma2.sae.query.rank;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
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
    public static List<DataStreamSink<RankAirportsResult>> buildAndAttach(DataStream<FlightRecord> mainStream, it.uniroma2.sae.config.ApplicationConfig config) {
        // Filter out cancelled and diverted flights at the entry of the pipeline to reduce network shuffle overhead
        DataStream<FlightRecord> activeFlightsStream = mainStream
                .filter(event -> event != null && !event.isCancelled() && !event.isDiverted())
                .name("Q2: Filter Active Flights")
                .uid("q2-filter-active-flights");

        KeyedStream<FlightRecord, Integer> keyedStream = activeFlightsStream.keyBy(FlightRecord::getOriginAirportId);
        KafkaConfig kafkaConfig = config.getKafka();
        Duration allowedLateness1h = Duration.ofMinutes(config.getFlink().getAllowedLatenessQ2_1hMinutes());
        Duration allowedLateness6h = Duration.ofMinutes(config.getFlink().getAllowedLatenessQ2_6hMinutes());
        Duration allowedLatenessGlobal = Duration.ofMinutes(config.getFlink().getAllowedLatenessQ2_globalMinutes());

        DataStream<RankAirportsResult> w1 = createTumblingWindowPipeline(keyedStream, Duration.ofHours(1), "1h", allowedLateness1h);
        DataStream<RankAirportsResult> w6 = createTumblingWindowPipeline(keyedStream, Duration.ofHours(6), "6h", allowedLateness6h);

        Duration globalTriggerInterval = Duration.ofHours(config.getFlink().getGlobalWindowTriggerHours());

        DataStream<RankAirportsResult> wGlobal = keyedStream
                .window(GlobalWindows.create())
                .trigger(ContinuousEventTimeTrigger.of(globalTriggerInterval))
                .allowedLateness(allowedLatenessGlobal)
                .aggregate(new RankAirportsAggregator(), new RankAirportsGlobalWindowProcessor(globalTriggerInterval))
                .name("Q2: Global Window")
                .uid("q2-global-window")
                .keyBy(RankAirportsResult::getWindowEnd)
                .process(new RankAirportsRankProcessor(allowedLatenessGlobal.plusSeconds(1)))
                .name("Q2: Global Rank")
                .uid("q2-global-rank");

        return List.of(
                attachKafkaSink(w1, kafkaConfig, "q2_1h", "Q2: 1h", "q2-sink-1h"),
                attachKafkaSink(w6, kafkaConfig, "q2_6h", "Q2: 6h", "q2-sink-6h"),
                attachKafkaSink(wGlobal, kafkaConfig, "q2_global", "Q2: Global", "q2-global-sink")
        );
    }

    private static DataStream<RankAirportsResult> createTumblingWindowPipeline(
            KeyedStream<FlightRecord, Integer> keyedStream,
            Duration windowSize,
            String label,
            Duration allowedLateness) {

        return keyedStream
                .window(TumblingEventTimeWindows.of(windowSize))
                .allowedLateness(allowedLateness)
                .aggregate(new RankAirportsAggregator(), new RankAirportsWindowProcessor(label))
                .name("Q2: Window (" + label + ")")
                .uid("q2-window-" + label)
                .keyBy(RankAirportsResult::getWindowEnd)
                .process(new RankAirportsRankProcessor(allowedLateness.plusSeconds(1)))
                .name("Q2: Rank (" + label + ")")
                .uid("q2-rank-" + label);
    }

    private static DataStreamSink<RankAirportsResult> attachKafkaSink(
            DataStream<RankAirportsResult> stream,
            KafkaConfig kafkaConfig,
            String queryKey,
            String pipelineName,
            String uid) {

        KafkaSink<RankAirportsResult> sink = KafkaSink.<RankAirportsResult>builder()
                .setBootstrapServers(kafkaConfig.getHost() + ":" + kafkaConfig.getInternalPort())
                .setRecordSerializer(new RankAirportsRecordSerializer(kafkaConfig, queryKey))
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        return stream.sinkTo(sink)
                .name(pipelineName + " Sink")
                .uid(uid);
    }

    /**
     * Serializer for sending RankAirports JSON strings to Kafka.
     * Roots message keys by origin_airport_id for ordered delivery.
     */
    public static class RankAirportsRecordSerializer implements KafkaRecordSerializationSchema<RankAirportsResult> {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        public RankAirportsRecordSerializer(KafkaConfig config, String queryKey) {
            this.topic = config.getOutputTopic(queryKey);
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                RankAirportsResult element, KafkaSinkContext ctx, Long timestamp) {

            byte[] key = String.valueOf(element.getOriginAirportId()).getBytes(StandardCharsets.UTF_8);
            byte[] value = null;

            try {
                value = MAPPER.writeValueAsBytes(element);
            } catch (Exception e) {
                return null;
            }

            return new ProducerRecord<>(topic, null, null, key, value);
        }
    }
}
