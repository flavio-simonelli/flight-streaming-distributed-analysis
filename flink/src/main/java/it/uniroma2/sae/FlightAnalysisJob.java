package it.uniroma2.sae;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.query.q1.Query1;
import it.uniroma2.sae.sink.SinkBuilder;
import it.uniroma2.sae.source.SourceBuilder;
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

        LOG.info("Connecting to Flink cluster at {}:{}...", flinkHost, flinkPort);

        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                flinkHost,
                flinkPort,
                "target/flight-analysis-1.0.jar"
        )) {
            // --- Shared KafkaSource ---
            KafkaSource<String> source = new SourceBuilder(config.getKafka()).build();

            // Assign event-time watermarks based on CRS_DEP_TIME embedded in each record.
            // BoundedOutOfOrderness accounts for late-arriving events injected by the simulator.
            WatermarkStrategy<FlightRecord> watermarkStrategy = WatermarkStrategy
                    .<FlightRecord>forBoundedOutOfOrderness(Duration.ofMinutes(10))
                    .withTimestampAssigner((event, ts) -> event.getEventTimeMillis());

            // --- Shared clean stream: raw JSON -> RawFlightRecord -> FlightRecord ---
            ObjectMapper mapper = new ObjectMapper();
            DataStream<FlightRecord> flightStream = env
                    .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka: flights-stream")
                    .flatMap(new FlightRecord.RawToFlightMapper(mapper))
                    .name("Deserialize & Clean")
                    .assignTimestampsAndWatermarks(watermarkStrategy)
                    .name("Watermark Assignment");

            // --- Attach query pipelines ---
            Query1 query1 = new Query1();
            DataStream<String> q1Stream = query1.build(flightStream);

            KafkaSink<String> sink = new SinkBuilder(config.getKafka())
                    .withRecordSerializer(new Query1.Q1RecordSerializer(config.getKafka()))
                    .build();
            
            q1Stream.sinkTo(sink).name("Q1: Kafka Sink -> " + config.getKafka().getOutputTopic("q1"));

            LOG.info("Submitting Flight Analysis Job...");
            env.execute("Flight Streaming Distributed Analysis");
        } catch (Exception e) {
            LOG.error("Flink job submission/execution failed: {}", e.getMessage(), e);
            throw e;
        }
    }
}
