package it.uniroma2.sae.query.distribution;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.FlinkConfig;
import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.metrics.LateRecordMetricAnalyzer;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.query.components.ActiveFlightFilter;
import it.uniroma2.sae.query.components.TargetAirlineFilter;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.util.OutputTag;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Set;

/**
 * Query 3 logic: Departure delay distribution (percentiles) per airline and hourly slot.
 * Computes statistical summaries concurrently over 1-day, 7-day tumbling windows,
 * and an unbounded global state pipeline.
 */
public class DelayDistributionQuery implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** The fixed size of the short-term tumbling event-time window. */
    private static final Duration WINDOW_SIZE_1D = Duration.ofDays(1);

    /** The fixed size of the medium-term tumbling event-time window. */
    private static final Duration WINDOW_SIZE_7D = Duration.ofDays(7);

    /** The immutable set of specific airline carrier codes targeted by this analysis query. */
    public static final Set<String> TARGET_AIRLINES = Set.of("AA", "DL", "UA", "WN");

    /**
     * Attaches the DelayDistributionQuery pipelines to the main preprocessed stream.
     * Maps the topology steps including dynamic filtering, key-based composite windowing,
     * and parallel incremental quantile evaluations.
     *
     * @param inputStream the clean, preprocessed incoming stream of FlightRecords
     * @param config the central application configuration container
     */
    public static void buildAndAttach(DataStream<FlightRecord> inputStream, ApplicationConfig config) {
        KafkaConfig kafkaConfig = config.getKafka();
        FlinkConfig flinkConfig = config.getFlink();

        // Dynamically fetch allowed lateness bounds for each concurrent window scope
        Duration allowedLateness1d = Duration.ofMinutes(flinkConfig.getAllowedLatenessQ3_1dMinutes());
        Duration allowedLateness7d = Duration.ofMinutes(flinkConfig.getAllowedLatenessQ3_7dMinutes());

        // Discard any flight record that does not belong to the target airline carriers
        DataStream<FlightRecord> targetAirlinesStream = inputStream
                .filter(new TargetAirlineFilter(TARGET_AIRLINES))
                .name("Q3: Filter Target Airlines")
                .uid("q3-filter-target-airlines");

        // Early filter to exclude canceled and diverted flights at the entry of the query
        // pipeline to dramatically reduce network shuffle and serialization overhead across the cluster
        DataStream<FlightRecord> activeFlightsStream = targetAirlinesStream
                .filter(new ActiveFlightFilter())
                .name("Q3: Filter Active Flights")
                .uid("q3-filter-active-flights");

        // Project the streams to a lightweight model containing only the fields of interest
        DataStream<DelayDistributionEvent> projectedStream = activeFlightsStream
                .map(DelayDistributionEvent::new)
                .name("Q3: Project to Lightweight Event")
                .uid("q3-project-lightweight-event")
                .returns(TypeInformation.of(DelayDistributionEvent.class));

        // Partition the distributed stream using a composite key consisting of Airline Code and Scheduled Departure Hour
        KeyedStream<DelayDistributionEvent, Tuple2<String, Integer>> keyedStream = projectedStream
                .keyBy(
                        event -> Tuple2.of(event.getAirline(), event.getScheduledDepartureHour()),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                );

        // Instantiate parallel tumbling execution sub-graphs via helper abstractions
        DataStream<DelayDistributionResult> w1d = createTumblingWindowPipeline(
                keyedStream, WINDOW_SIZE_1D, allowedLateness1d, "1d", "q3-late-flights-1d", "q3-late-analyzer-1d", "q3-window-1d"
        );

        DataStream<DelayDistributionResult> w7d = createTumblingWindowPipeline(
                keyedStream, WINDOW_SIZE_7D, allowedLateness7d, "7d", "q3-late-flights-7d", "q3-late-analyzer-7d", "q3-window-7d"
        );

        // Map the infinite Global Window execution graph triggered periodically every 1 day
        DataStream<DelayDistributionResult> wGlobal = keyedStream
                .window(GlobalWindows.create())
                .trigger(ContinuousEventTimeTrigger.of(WINDOW_SIZE_1D))
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionGlobalWindowProcessor())
                .name("Q3: Global Window")
                .uid("q3-global-window");

        // Finalize graph routes linking each specialized stream scope directly to its dedicated Kafka endpoint
        attachKafkaSink(w1d, kafkaConfig, "q3_1d", "Q3: 1d", "q3-sink-1d");
        attachKafkaSink(w7d, kafkaConfig, "q3_7d", "Q3: 7d", "q3-sink-7d");
        attachKafkaSink(wGlobal, kafkaConfig, "q3_global", "Q3: Global", "q3-sink-global");
    }

    /**
     * Internal factory method encapsulating tumbling window generation, state retention strategies,
     * side-output tracking for late-arriving simulation data, and continuous aggregation execution.
     */
    private static DataStream<DelayDistributionResult> createTumblingWindowPipeline(
            KeyedStream<DelayDistributionEvent, Tuple2<String, Integer>> keyedStream,
            Duration windowSize,
            Duration allowedLateness,
            String label,
            String lateTagId,
            String analyzerUid,
            String windowUid) {

        OutputTag<DelayDistributionEvent> lateTag = new OutputTag<>(lateTagId, TypeInformation.of(DelayDistributionEvent.class));

        SingleOutputStreamOperator<DelayDistributionResult> windowedOperator = keyedStream
                .window(TumblingEventTimeWindows.of(windowSize))
                .allowedLateness(allowedLateness)
                .sideOutputLateData(lateTag)
                .aggregate(new DelayDistributionAggregator(), new DelayDistributionWindowProcessor(label));

        // Track and analyze late-arriving events redirected to the side output side-channel
        LateRecordMetricAnalyzer.attachSideOutput(
                windowedOperator,
                lateTag,
                windowSize,
                allowedLateness,
                "Q3: Late Records Metric Analyzer (" + label + ")",
                analyzerUid
                );

        return windowedOperator
                .name("Q3: Window (" + label + ")")
                .uid(windowUid);
    }

    /**
     * Direct factory initializer linking processing stream output definitions directly to functional physical Kafka Sinks.
     */
    private static void attachKafkaSink(
            DataStream<DelayDistributionResult> stream,
            KafkaConfig kafkaConfig,
            String queryKey,
            String pipelineName,
            String uid) {

        KafkaSink<DelayDistributionResult> sink = new SinkBuilder<DelayDistributionResult>(kafkaConfig)
                .withRecordSerializer(new DelayDistributionResult.DelayDistributionRecordSerializer(kafkaConfig, queryKey))
                .build();

        stream.sinkTo(sink)
                .name(pipelineName + " Sink")
                .uid(uid);
    }

}
