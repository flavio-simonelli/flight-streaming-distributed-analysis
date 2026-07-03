package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.preprocessing.PipelinePreprocessing;
import it.uniroma2.sae.query.distribution.DelayDistributionQuery;
import it.uniroma2.sae.query.performance.AirlinePerformanceQuery;
import it.uniroma2.sae.query.rank.RankAirportsQuery;
import it.uniroma2.sae.source.SourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.execution.CheckpointingMode;
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
        // Load application configuration
        ApplicationConfig config = ApplicationConfig.load("application.yaml");

        // Initialize execution environment and checkpoint configuration
        StreamExecutionEnvironment env = initExecutionEnvironment(config);
        initCheckpointing(env, config.getFlink().getCheckpoint());

        // Define event-time watermark strategy
        WatermarkStrategy<FlightRecord> watermarkStrategy = initWatermarkStrategy(config);

        // Build primary source and clean stream
        KafkaSource<FlightRecord> source = new SourceBuilder(config.getKafka()).build();
        DataStream<FlightRecord> rawStream = env
                .fromSource(source, watermarkStrategy, config.getKafka().getInputTopic() + "-source")
                .name("Kafka Source")
                .uid("kafka-source");

        DataStream<FlightRecord> preprocessedStream = PipelinePreprocessing.preprocess(rawStream);

        // Attach analytical query pipelines
        AirlinePerformanceQuery.buildAndAttach(preprocessedStream, config);
        RankAirportsQuery.buildAndAttach(preprocessedStream, config);
        DelayDistributionQuery.buildAndAttach(preprocessedStream, config);

        // Execute Flink Job
        try {
            LOG.info("Submitting Flight Analysis Job...");
            env.execute("Flight Streaming Distributed Analysis");
        } catch (Exception e) {
            LOG.error("Flink job submission/execution failed: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Initializes the StreamExecutionEnvironment.
     * Decides whether to submit remotely to an active JobManager or run locally
     * based on the presence of the FLINK_CONF_DIR environment variable.
     */
    private static StreamExecutionEnvironment initExecutionEnvironment(ApplicationConfig config) {
        StreamExecutionEnvironment env;
        String flinkHost = config.getFlink().getHost();
        int flinkPort = config.getFlink().getPort();

        if (System.getenv("FLINK_CONF_DIR") != null) {
            LOG.info("Detected Flink cluster environment. Using default execution environment.");
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            LOG.info("No Flink cluster environment detected. Connecting to Flink cluster at {}:{} to submit remotely...", flinkHost, flinkPort);
            env = StreamExecutionEnvironment.createRemoteEnvironment(
                    flinkHost,
                    flinkPort,
                    "target/flight-analysis-1.0.jar"
            );
        }

        if (config.getFlink().getParallelism() > 0) {
            env.setParallelism(config.getFlink().getParallelism());
        }

        if (config.getFlink().getMaxParallelism() > 0) {
            env.setMaxParallelism(config.getFlink().getMaxParallelism());
        }

        return env;
    }

    /**
     * Sets up Flink's checkpointing system using the official StreamExecutionEnvironment APIs.
     */
    private static void initCheckpointing(
            StreamExecutionEnvironment env,
            it.uniroma2.sae.config.CheckpointConfig checkpointCfg) {

        if (checkpointCfg == null || !checkpointCfg.isEnabled()) {
            LOG.info("Flink checkpointing is disabled.");
            return;
        }

        LOG.info("Configuring Flink checkpointing...");

        Configuration flinkConfig = new Configuration();

        flinkConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(checkpointCfg.getIntervalMillis()));
        flinkConfig.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
        flinkConfig.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMillis(checkpointCfg.getMinPauseMillis()));
        flinkConfig.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMillis(checkpointCfg.getTimeoutMillis()));
        flinkConfig.set(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, checkpointCfg.getTolerableFailedCheckpoints());
        flinkConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, checkpointCfg.isUnalignedCheckpoints());
        flinkConfig.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, checkpointCfg.getMaxConcurrentCheckpoints());
        configureCheckpointStorage(flinkConfig, checkpointCfg);

        env.configure(flinkConfig);
    }

    /**
     * Helper to set up the WatermarkStrategy based on configuration settings.
     */
    private static WatermarkStrategy<FlightRecord> initWatermarkStrategy(ApplicationConfig config) {
        WatermarkStrategy<FlightRecord> strategy = WatermarkStrategy
                .<FlightRecord>forBoundedOutOfOrderness(Duration.ofMinutes(config.getFlink().getWatermarkDelayMinutes()))
                .withTimestampAssigner((event, ts) -> event.getEventTimeMillis());

        if (config.getFlink().getWatermarkIdlenessMinutes() > 0) {
            strategy = strategy.withIdleness(Duration.ofMinutes(config.getFlink().getWatermarkIdlenessMinutes()));
        }

        return strategy;
    }

    /**
     * Configures the target filesystem storage for Checkpoints.
     */
    private static void configureCheckpointStorage(
            Configuration flinkConfig,
            it.uniroma2.sae.config.CheckpointConfig cpCfg) {

        if (cpCfg == null || cpCfg.getStorageType() == null || cpCfg.getStorageType().isBlank()) {
            LOG.info("No checkpoint storage type configured — using Flink default.");
            return;
        }

        String storageType = cpCfg.getStorageType().trim().toLowerCase();
        String checkpointUri;

        switch (storageType) {
            case "hdfs": {
                it.uniroma2.sae.config.CheckpointConfig.HdfsConfig hdfs = cpCfg.getHdfs();
                if (hdfs == null || hdfs.getNamenode() == null || hdfs.getNamenode().isBlank()) {
                    throw new IllegalArgumentException(
                            "flink.checkpoint.hdfs.namenode must be specified when storageType=hdfs");
                }

                String namenode = hdfs.getNamenode().replaceAll("/+$", "");
                String path = cpCfg.getPath() != null ? cpCfg.getPath() : "/flink/checkpoints";
                if (!path.startsWith("/")) path = "/" + path;

                checkpointUri = namenode + path;
                LOG.info("Checkpoint storage: HDFS — {}", checkpointUri);
                break;
            }

            case "s3": {
                it.uniroma2.sae.config.CheckpointConfig.S3Config s3 = cpCfg.getS3();
                if (s3 == null || s3.getBucket() == null || s3.getBucket().isBlank()) {
                    throw new IllegalArgumentException(
                            "flink.checkpoint.s3.bucket must be specified when storageType=s3");
                }

                String bucket = s3.getBucket().trim();
                String path = cpCfg.getPath() != null
                        ? cpCfg.getPath().replaceAll("^/+", "")
                        : "flink/checkpoints";

                checkpointUri = "s3a://" + bucket + "/" + path;

                if (s3.getAccessKey() != null && !s3.getAccessKey().isBlank()) {
                    System.setProperty("aws.accessKeyId", s3.getAccessKey());
                    System.setProperty("aws.secretKey", s3.getSecretKey());
                    LOG.info("S3 checkpoint storage: using explicit credentials.");
                } else {
                    LOG.info("S3 checkpoint storage: no explicit credentials — using IAM Instance Profile.");
                }

                if (s3.getRegion() != null && !s3.getRegion().isBlank()) {
                    System.setProperty("aws.region", s3.getRegion());
                }

                LOG.info("Checkpoint storage: S3 — {}", checkpointUri);
                break;
            }

            case "local": {
                String path = cpCfg.getPath();
                if (path == null || path.isBlank()) {
                    throw new IllegalArgumentException(
                            "flink.checkpoint.path must be specified when storageType=local");
                }

                checkpointUri = path.startsWith("file://") ? path : "file://" + path;
                LOG.info("Checkpoint storage: local FS — {}", checkpointUri);
                break;
            }

            default:
                throw new IllegalArgumentException(
                        "Unknown flink.checkpoint.storageType: '" + storageType
                                + "'. Supported values: hdfs, s3, local");
        }

        flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointUri);
    }
}
