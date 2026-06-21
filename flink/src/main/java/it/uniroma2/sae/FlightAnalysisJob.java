package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.preprocessing.PipelinePreprocessing;
import it.uniroma2.sae.query.q1.Query1;
import it.uniroma2.sae.query.q2.Query2;
import it.uniroma2.sae.source.SourceBuilder;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

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
            // The watermark strategy is MIN from all parallel sources and wait all the sources
            // to emit a watermark before advancing the global watermark.
            WatermarkStrategy<FlightRecord> watermarkStrategy = WatermarkStrategy
                    .<FlightRecord>forBoundedOutOfOrderness(Duration.ofMinutes(10)) //TODO: fine tuning
                    .withTimestampAssigner((event, ts) -> event.getEventTimeMillis());
            
            KafkaSource<FlightRecord> source = new SourceBuilder(config.getKafka()).build();

            DataStream<FlightRecord> rawStream = env
                    .fromSource(source, WatermarkStrategy.noWatermarks(), "flights-stream").name("Kafka Source")
                    .assignTimestampsAndWatermarks(watermarkStrategy).name("Watermarks");;

            DataStream<FlightRecord> preprocessedStream = PipelinePreprocessing.preprocess(rawStream);

            // --- Attach query pipelines ---
            DataStreamSink<String> q1Pipeline = Query1.buildAndAttach(preprocessedStream, config.getKafka());

            List<DataStreamSink<String>> q2Pipelines = Query2.buildAndAttach(preprocessedStream, config.getKafka());

            LOG.info("Submitting Flight Analysis Job...");
            env.execute("Flight Streaming Distributed Analysis");
        } catch (Exception e) {
            LOG.error("Flink job submission/execution failed: {}", e.getMessage(), e);
            throw e;
        }
    }
}
