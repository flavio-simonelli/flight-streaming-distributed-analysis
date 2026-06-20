package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.query.q1.Query1;
import it.uniroma2.sae.sink.SinkBuilder;
import it.uniroma2.sae.source.SourceBuilder;
import it.uniroma2.sae.source.deserializer.FlightRecordDeserializationSchema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Entry point for the Flight Analysis Flink application.
 * Sets up the shared stream infrastructure and attaches all query pipelines.
 */
public class FlightAnalysisJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlightAnalysisJob.class);

    public static void main(String[] args) throws Exception {
        ApplicationConfig config = ApplicationConfig.load("application.yaml");

        String flinkHost = config.getFlink().getHost();
        int flinkPort = config.getFlink().getPort();

        StreamExecutionEnvironment env;
        if (System.getenv("FLINK_CONF_DIR") != null) {
            LOG.info("Detected Flink cluster environment.");
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            LOG.info("No Flink cluster environment detected. Connecting to Flink cluster at {}:{} to submit remotely...", flinkHost, flinkPort);
            env = StreamExecutionEnvironment.createRemoteEnvironment(
                    flinkHost,
                    flinkPort,
                    "target/flight-analysis-1.0.jar"
            );
        }

        try (StreamExecutionEnvironment envToClose = env) {

            // Assign event-time watermarks based on CRS_DEP_TIME embedded in each record.
            // BoundedOutOfOrderness accounts for late-arriving events injected by the simulator.
            // The watermark strategy is MIN from all parallel sources and wait all the sources to emit a watermark before advancing the global watermark.
            WatermarkStrategy<FlightRecord> watermarkStrategy = WatermarkStrategy
                    .<FlightRecord>forBoundedOutOfOrderness(Duration.ofMinutes(10)) //TODO: fine tuning
                    .withTimestampAssigner((event, ts) -> event.getEventTimeMillis());
            
            KafkaSource<FlightRecord> source = new SourceBuilder<FlightRecord>(
                    config.getKafka(), 
                    new FlightRecordDeserializationSchema()
            ).build();

            DataStream<FlightRecord> flightStream = env
                .fromSource(source, watermarkStrategy, "Kafka: flights-stream")
                .name("Kafka Source & Watermarks");

            // --- Attach query pipelines ---
            Query1 query1 = new Query1();
            DataStream<String> q1Stream = query1.build(flightStream);

            KafkaSink<String> sink = new SinkBuilder(config.getKafka())
                    .withRecordSerializer(new Query1.Q1RecordSerializer(config.getKafka()))
                    .build();

            // --- Shared KafkaSink ---
            q1Stream.sinkTo(sink).name("Q1: Kafka Sink -> " + config.getKafka().getOutputTopic("q1"));

            LOG.info("Submitting Flight Analysis Job...");
            env.execute("Flight Streaming Distributed Analysis");
        } catch (Exception e) {
            LOG.error("Flink job submission/execution failed: {}", e.getMessage(), e);
            throw e;
        }
    }
}
