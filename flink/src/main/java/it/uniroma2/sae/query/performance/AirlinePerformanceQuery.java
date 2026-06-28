package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.FlinkConfig;
import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.metrics.LateRecordMetricAnalyzer;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.query.components.TargetAirlineFilter;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * Query 1 logic: Airline operational performance metrics.
 * Runs on 1-hour tumbling windows over target carrier events.
 */
public class AirlinePerformanceQuery implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** The fixed size of the tumbling event-time window. */
    private static final Duration WINDOW_SIZE = Duration.ofHours(1);

    /** The immutable set of specific airline carrier codes targeted by this analysis query. */
    public static final Set<String> TARGET_AIRLINES = Set.of("AA", "DL", "UA", "WN");

    private static final OutputTag<FlightRecord> LATE_FLIGHTS_TAG =
            new OutputTag<>("q1-late-flights", TypeInformation.of(FlightRecord.class));

    /**
     * Attaches the AirlinePerformanceQuery pipeline to the main preprocessed stream.
     * Maps the topology steps including filtering, key-based windowing, and streaming aggregation.
     *
     * @param inputStream the clean, preprocessed incoming stream of FlightRecords
     * @param config the central application configuration container
     * @return a list containing the data stream sinks attached to this query endpoint
     */
    public static List<DataStreamSink<AirlinePerformanceResult>> buildAndAttach(DataStream<FlightRecord> inputStream, ApplicationConfig config) {
        KafkaConfig kafkaConfig = config.getKafka();
        FlinkConfig  flinkConfig = config.getFlink();

        // Dynamically fetch the allowed lateness duration specified in the YAML configuration
        Duration allowedLateness = Duration.ofMinutes(flinkConfig.getAllowedLatenessQ1Minutes());

        // Discard any flight record that does not belong to the target airline carriers
        DataStream<FlightRecord> targetAirlinesStream = inputStream
                .filter(new TargetAirlineFilter(TARGET_AIRLINES))
                .name("Q1: Filter Target Airlines")
                .uid("q1-filter-target-airlines");

        // Route elements by carrier, open 1-hour windows, apply lateness tolerance, and compute metrics
        SingleOutputStreamOperator<AirlinePerformanceResult> windowedOperator = targetAirlinesStream
                .keyBy(FlightRecord::getAirline)                                                        // Dynamically partition the stream by the airline unique code
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))                                       // Segment the event timeline into non-overlapping 1-hour blocks
                .allowedLateness(allowedLateness)                                                       // Keep windows alive in memory for late records from the simulator
                .sideOutputLateData(LATE_FLIGHTS_TAG)
                .aggregate(new AirlinePerformanceAggregator(), new AirlinePerformanceWindowProcessor()); // Incremental aggregation with context

        windowedOperator.getSideOutput(LATE_FLIGHTS_TAG)
                .process(new LateRecordMetricAnalyzer(WINDOW_SIZE, allowedLateness))
                .name("Q1: Late Records Metric Analyzer")
                .uid("q1-late-analyzer");

        DataStream<AirlinePerformanceResult> stream = windowedOperator
                .name("Q1: Performance")
                .uid("q1-window-performance");

        // Instantiate the persistent storage mechanism targeting the dedicated Kafka sink topic
        KafkaSink<AirlinePerformanceResult> sink = new SinkBuilder<AirlinePerformanceResult>(kafkaConfig)
                .withRecordSerializer(new AirlinePerformanceResult.AirlinePerformanceRecordSerializer(kafkaConfig))
                .build();

        // Finalize the graph execution route by linking the processed stream directly to the Kafka sink
        return List.of(
                stream.sinkTo(sink)
                        .name("Q1: Sink")
                        .uid("q1-sink")
        );
    }
}