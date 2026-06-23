package it.uniroma2.sae.query.performance;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.query.common.BaseAirlineQuery;
import it.uniroma2.sae.sink.SinkBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.List;

/**
 * Query 1 logic: Airline operational performance metrics.
 * Runs on 1-hour tumbling windows over target carrier events.
 */
public class AirlinePerformanceQuery extends BaseAirlineQuery {
    private static final Duration WINDOW_SIZE = Duration.ofHours(1);

    /**
     * Attaches the AirlinePerformanceQuery pipeline to the pre-filtered target airlines stream.
     */
    public static List<DataStreamSink<AirlinePerformanceResult>> buildAndAttach(DataStream<FlightRecord> targetAirlinesStream, ApplicationConfig config) {
        KafkaConfig kafkaConfig = config.getKafka();
        Duration allowedLateness = Duration.ofMinutes(config.getFlink().getAllowedLatenessMinutes());

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
