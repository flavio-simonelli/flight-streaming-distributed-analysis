package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.CheckpointConfig;
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

        if (config.getFlink().getMaxParallelism() > 0) {
            env.setMaxParallelism(config.getFlink().getMaxParallelism());
        }

        CheckpointConfig checkpointCfg = config.getFlink().getCheckpoint();

        // Enables and configures Flink's checkpointing mechanism (Fault Tolerance).
        // If enabled in the application properties, this block:
        // - Activates periodic checkpointing with the specified time interval.
        // - Sets the data consistency mode to EXACTLY_ONCE (guarantees no data loss, not allowing duplicates).
        // - Defines a minimum pause between checkpoints to prevent system overload (checkpoint backpressure).
        // - Establishes a maximum timeout after which a pending checkpoint is aborted.
        // - Initializes and applies the target storage backend (HDFS, S3, or Local) based on configuration.
        if (checkpointCfg != null && checkpointCfg.isEnabled()) {
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
                .fromSource(source, watermarkStrategy, "flights-stream")
                .name("Kafka Source")
                .uid("kafka-source");

        DataStream<FlightRecord> preprocessedStream = PipelinePreprocessing.preprocess(rawStream);

        // --- Attach query pipelines ---
        AirlinePerformanceQuery.buildAndAttach(preprocessedStream, config);

        RankAirportsQuery.buildAndAttach(preprocessedStream, config);

        DelayDistributionQuery.buildAndAttach(preprocessedStream, config);

        try {
            LOG.info("Submitting Flight Analysis Job...");
            env.execute("Flight Streaming Distributed Analysis");
        } catch (Exception e) {
            LOG.error("Flink job submission/execution failed: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Configures the Flink checkpoint storage backend based on application.yaml settings.
     *
     * <ul>
     *   <li><b>hdfs</b> — saves to HDFS. The {@code dfs.client.use.datanode.hostname=true}
     *       property is loaded automatically from {@code hdfs-site.xml} bundled in the
     *       fat-jar classpath (src/main/resources/hdfs-site.xml), so no internal Flink or
     *       Hadoop APIs are needed here.</li>
     *   <li><b>s3</b> — saves to Amazon S3 via the {@code s3a://} scheme (Hadoop-AWS).
     *       Explicit credentials are injected as AWS SDK system properties ({@code aws.accessKeyId},
     *       {@code aws.secretKey}, {@code aws.region}) so the standard credential provider
     *       chain picks them up. When credentials are absent, the EC2 IAM Instance Profile
     *       is used automatically.</li>
     *   <li><b>local</b> — saves to a local filesystem path (useful for testing).</li>
     *   <li>null / empty — no explicit storage configured; Flink uses its default.</li>
     * </ul>
     *
     * @param flinkConfig the Flink {@link Configuration} to set storage options on
     * @param cpCfg       the checkpoint storage configuration read from YAML (may be null)
     */
    private static void configureCheckpointStorage(
            Configuration flinkConfig,
            CheckpointConfig cpCfg) {

        if (cpCfg == null || cpCfg.getStorageType() == null || cpCfg.getStorageType().isBlank()) {
            LOG.info("No checkpoint storage type configured — using Flink default (local JobManager FS).");
            return;
        }

        String storageType = cpCfg.getStorageType().trim().toLowerCase();
        String checkpointUri;

        switch (storageType) {
            case "hdfs": {
                CheckpointConfig.HdfsConfig hdfs = cpCfg.getHdfs();
                if (hdfs == null || hdfs.getNamenode() == null || hdfs.getNamenode().isBlank()) {
                    throw new IllegalArgumentException(
                            "flink.checkpoint.hdfs.namenode must be specified when storageType=hdfs");
                }
                // Build the full HDFS URI: hdfs://<namenode><path>
                // The Hadoop client discovers the NameNode from the URI directly — no need
                // for fs.defaultFS. The dfs.client.use.datanode.hostname property (required
                // to resolve DataNode hostnames inside Docker) is loaded from hdfs-site.xml
                // placed in src/main/resources, which Hadoop's Configuration reads automatically.
                String namenode = hdfs.getNamenode().replaceAll("/+$", "");
                String path = cpCfg.getPath() != null ? cpCfg.getPath() : "/flink/checkpoints";
                if (!path.startsWith("/")) path = "/" + path;
                checkpointUri = namenode + path;
                LOG.info("Checkpoint storage: HDFS — {}", checkpointUri);
                break;
            }

            case "s3": {
                CheckpointConfig.S3Config s3 = cpCfg.getS3();
                if (s3 == null || s3.getBucket() == null || s3.getBucket().isBlank()) {
                    throw new IllegalArgumentException(
                            "flink.checkpoint.s3.bucket must be specified when storageType=s3");
                }
                String bucket = s3.getBucket().trim();
                String path = cpCfg.getPath() != null
                        ? cpCfg.getPath().replaceAll("^/+", "")
                        : "flink/checkpoints";
                checkpointUri = "s3a://" + bucket + "/" + path;

                // Credentials are injected as standard AWS SDK system properties.
                // The AWS SDK credential provider chain reads these before falling back
                // to environment variables and the EC2 Instance Metadata Service.
                if (s3.getAccessKey() != null && !s3.getAccessKey().isBlank()) {
                    System.setProperty("aws.accessKeyId", s3.getAccessKey());
                    System.setProperty("aws.secretKey", s3.getSecretKey());
                    LOG.info("S3 checkpoint storage: using explicit credentials.");
                } else {
                    LOG.info("S3 checkpoint storage: no explicit credentials — using IAM Instance Profile.");
                }
                // Region can also be picked up from AWS_DEFAULT_REGION env var on EC2,
                // but an explicit value in the YAML takes priority.
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
