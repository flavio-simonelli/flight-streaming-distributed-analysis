package it.uniroma2.sae.query.performance;

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
    private static final Duration ALLOWED_LATENESS = Duration.ofMinutes(5);

    /**
     * Attaches the AirlinePerformanceQuery pipeline to the pre-filtered target airlines stream.
     */
    public static List<DataStreamSink<String>> buildAndAttach(DataStream<FlightRecord> targetAirlinesStream, KafkaConfig kafkaConfig) {
        DataStream<String> stream = targetAirlinesStream
                .keyBy(FlightRecord::getAirline)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .allowedLateness(ALLOWED_LATENESS)
                .aggregate(new AirlinePerformanceAggregator(), new AirlinePerformanceWindowProcessor())
                .name("Q1: Performance");

        KafkaSink<String> sink = new SinkBuilder(kafkaConfig)
                .withRecordSerializer(new AirlinePerformanceOutput.AirlinePerformanceRecordSerializer(kafkaConfig))
                .build();

        return List.of(
                stream.sinkTo(sink).name("Q1: Sink")
        );
    }

}
