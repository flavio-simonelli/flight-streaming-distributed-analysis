package it.uniroma2.sae.query.distribution;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.metrics.LateRecordMetricAnalyzer;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * Query 3 logic: Departure delay distribution (percentiles) per airline and hourly slot.
 * Runs concurrently over 1d, 7d tumbling windows, and global state.
 */
public class DelayDistributionQuery implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final Set<String> TARGET_AIRLINES = Set.of("AA", "DL", "UA", "WN");

    private static final OutputTag<FlightRecord> LATE_FLIGHTS_1D_TAG =
            new OutputTag<>("q3-late-flights-1d", TypeInformation.of(FlightRecord.class));
    private static final OutputTag<FlightRecord> LATE_FLIGHTS_7D_TAG =
            new OutputTag<>("q3-late-flights-7d", TypeInformation.of(FlightRecord.class));

    /**
     * Dedicated FilterFunction to isolate target carriers.
     */
    public static class TargetAirlineFilter implements FilterFunction<FlightRecord> {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(FlightRecord event) throws Exception {
            return event != null && TARGET_AIRLINES.contains(event.getAirline());
        }
    }

    /**
     * Builds and attaches the DelayDistributionQuery pipelines to the input stream.
     */
    public static List<DataStreamSink<DelayDistributionResult>> buildAndAttach(DataStream<FlightRecord> inputStream, ApplicationConfig config) {
        KafkaConfig kafkaConfig = config.getKafka();
        Duration allowedLateness1d = Duration.ofMinutes(config.getFlink().getAllowedLatenessQ3_1dMinutes());
        Duration allowedLateness7d = Duration.ofMinutes(config.getFlink().getAllowedLatenessQ3_7dMinutes());

        DataStream<FlightRecord> targetAirlinesStream = inputStream
                .filter(new TargetAirlineFilter())
                .name("Q3: Filter Target Airlines")
                .uid("q3-filter-target-airlines");

        // Key by airline and hour
        KeyedStream<FlightRecord, Tuple2<String, Integer>> keyedStream = targetAirlinesStream
                .keyBy(
                        event -> Tuple2.of(event.getAirline(), event.getScheduledDepartureHour()),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                );

        SingleOutputStreamOperator<DelayDistributionResult> windowedOperator1d = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .allowedLateness(allowedLateness1d)
                .sideOutputLateData(LATE_FLIGHTS_1D_TAG)
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionWindowProcessor("1d"));

        windowedOperator1d.getSideOutput(LATE_FLIGHTS_1D_TAG)
                .process(new LateRecordMetricAnalyzer(Duration.ofDays(1), allowedLateness1d))
                .name("Q3: Late Records Metric Analyzer (1d)")
                .uid("q3-late-analyzer-1d");

        DataStream<DelayDistributionResult> w1d = windowedOperator1d
                .name("Q3: Window (1d)")
                .uid("q3-window-1d");

        SingleOutputStreamOperator<DelayDistributionResult> windowedOperator7d = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofDays(7)))
                .allowedLateness(allowedLateness7d)
                .sideOutputLateData(LATE_FLIGHTS_7D_TAG)
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionWindowProcessor("7d"));

        windowedOperator7d.getSideOutput(LATE_FLIGHTS_7D_TAG)
                .process(new LateRecordMetricAnalyzer(Duration.ofDays(7), allowedLateness7d))
                .name("Q3: Late Records Metric Analyzer (7d)")
                .uid("q3-late-analyzer-7d");

        DataStream<DelayDistributionResult> w7d = windowedOperator7d
                .name("Q3: Window (7d)")
                .uid("q3-window-7d");

        DataStream<DelayDistributionResult> wGlobal = keyedStream
                .window(GlobalWindows.create())
                .trigger(ContinuousEventTimeTrigger.of(Duration.ofDays(1)))
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionGlobalWindowProcessor())
                .name("Q3: Global Window")
                .uid("q3-global-window");

        return List.of(
                attachKafkaSink(w1d, kafkaConfig, "q3_1d", "Q3: 1d", "q3-sink-1d"),
                attachKafkaSink(w7d, kafkaConfig, "q3_7d", "Q3: 7d", "q3-sink-7d"),
                attachKafkaSink(wGlobal, kafkaConfig, "q3_global", "Q3: Global", "q3-sink-global")
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