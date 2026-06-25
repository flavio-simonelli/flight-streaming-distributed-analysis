package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.preprocessing.PipelinePreprocessing;
import it.uniroma2.sae.query.distribution.DelayDistributionQuery;
import it.uniroma2.sae.query.performance.AirlinePerformanceQuery;
import it.uniroma2.sae.query.rank.RankAirportsQuery;
import it.uniroma2.sae.source.SourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
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
            if (config.getFlink().isCheckpointingEnabled()) {
                env.enableCheckpointing(config.getFlink().getCheckpointIntervalMillis(), CheckpointingMode.AT_LEAST_ONCE);
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.getFlink().getMinPauseBetweenCheckpointsMillis());
                env.getCheckpointConfig().setCheckpointTimeout(config.getFlink().getCheckpointTimeoutMillis());
            }

            // Assign event-time watermarks based on CRS_DEP_TIME embedded in each record.
            // BoundedOutOfOrderness accounts for late-arriving events injected by the simulator.
            // The watermark strategy is MIN from all parallel sources and wait all the sources
            // to emit a watermark before advancing the global watermark.
            WatermarkStrategy<FlightRecord> watermarkStrategy = WatermarkStrategy
                    .<FlightRecord>forBoundedOutOfOrderness(Duration.ofMinutes(config.getFlink().getWatermarkDelayMinutes()))
                    .withTimestampAssigner((event, ts) -> event.getEventTimeMillis());

            if (config.getFlink().getWatermarkIdlenessMinutes() > 0) {
                watermarkStrategy = watermarkStrategy.withIdleness(Duration.ofMinutes(config.getFlink().getWatermarkIdlenessMinutes()));
            }
            
            KafkaSource<FlightRecord> source = new SourceBuilder(config.getKafka()).build();

            DataStream<FlightRecord> rawStream = env
                    .fromSource(source, WatermarkStrategy.noWatermarks(), "flights-stream")
                    .name("Kafka Source")
                    .uid("kafka-source");

            DataStream<FlightRecord> preprocessedStream = PipelinePreprocessing.preprocess(rawStream)
                    .assignTimestampsAndWatermarks(watermarkStrategy)
                    .name("Watermarks")
                    .uid("watermark-assigner");

            // --- Attach query pipelines ---
            List<DataStreamSink<it.uniroma2.sae.query.performance.AirlinePerformanceResult>> q1Pipeline = AirlinePerformanceQuery.buildAndAttach(preprocessedStream, config);

            List<DataStreamSink<it.uniroma2.sae.query.rank.RankAirportsResult>> q2Pipelines = RankAirportsQuery.buildAndAttach(preprocessedStream, config);

            List<DataStreamSink<it.uniroma2.sae.query.distribution.DelayDistributionResult>> q3Pipelines = DelayDistributionQuery.buildAndAttach(preprocessedStream, config);

            LOG.info("Submitting Flight Analysis Job...");
            env.execute("Flight Streaming Distributed Analysis");
        } catch (Exception e) {
            LOG.error("Flink job submission/execution failed: {}", e.getMessage(), e);
            throw e;
        }
    }
}
