package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

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

    private static final Duration WINDOW_SIZE = Duration.ofHours(1);
    public static final Set<String> TARGET_AIRLINES = Set.of("AA", "DL", "UA", "WN");

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
     * Attaches the AirlinePerformanceQuery pipeline to the main preprocessed stream.
     */
    public static List<DataStreamSink<AirlinePerformanceResult>> buildAndAttach(DataStream<FlightRecord> inputStream, ApplicationConfig config) {
        KafkaConfig kafkaConfig = config.getKafka();
        Duration allowedLateness = Duration.ofMinutes(config.getFlink().getAllowedLatenessMinutes());

        DataStream<FlightRecord> targetAirlinesStream = inputStream
                .filter(new TargetAirlineFilter())
                .name("Q1: Filter Target Airlines")
                .uid("q1-filter-target-airlines");

        DataStream<AirlinePerformanceResult> stream = targetAirlinesStream
                .keyBy(FlightRecord::getAirline)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .allowedLateness(allowedLateness)
                .aggregate(new AirlinePerformanceAggregator(), new AirlinePerformanceWindowProcessor())
                .name("Q1: Performance")
                .uid("q1-window-performance");

        KafkaSink<AirlinePerformanceResult> sink = new SinkBuilder<AirlinePerformanceResult>(kafkaConfig)
                .withRecordSerializer(new AirlinePerformanceResult.AirlinePerformanceRecordSerializer(kafkaConfig))
                .build();

        return List.of(
                stream.sinkTo(sink)
                        .name("Q1: Sink")
                        .uid("q1-sink")
        );
    }
}
