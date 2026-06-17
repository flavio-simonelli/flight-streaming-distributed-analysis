package it.uniroma2.sae;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.model.RawFlightRecord;
import it.uniroma2.sae.query.q1.Query1;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Flight Analysis Flink application.
 * Sets up the shared stream infrastructure and attaches all query pipelines.
 */
public class FlightAnalysisJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlightAnalysisJob.class);

    public static void main(String[] args) throws Exception {
        ApplicationConfig config = ApplicationConfig.load("application.yaml");

        String kafkaBootstrap = config.getKafka().getHost() + ":" + config.getKafka().getInternalPort();
        String inputTopic     = config.getKafka().getInputTopic();
        String groupId        = config.getKafka().getGroupId();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Shared KafkaSource (single source, fork pattern) ---
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Assign event-time watermarks based on CRS_DEP_TIME embedded in each record.
        // BoundedOutOfOrderness accounts for late-arriving events injected by the simulator.
        WatermarkStrategy<FlightRecord> watermarkStrategy = WatermarkStrategy
                .<FlightRecord>forBoundedOutOfOrderness(java.time.Duration.ofMinutes(10))
                .withTimestampAssigner((event, ts) -> event.getEventTimeMillis());

        // --- Shared clean stream: raw JSON -> RawFlightRecord -> FlightRecord ---
        ObjectMapper mapper = new ObjectMapper();
        DataStream<FlightRecord> flightStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka: flights-stream")
                .flatMap(new RawToFlightMapper(mapper))
                .name("Deserialize & Clean")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("Watermark Assignment");

        // --- Attach query pipelines (fork) ---
        new Query1(config, kafkaBootstrap).build(flightStream);

        LOG.info("Submitting Flight Analysis Job...");
        env.execute("Flight Streaming Distributed Analysis");
    }

    /**
     * FlatMapFunction that deserializes a raw JSON string from Kafka into a clean FlightRecord.
     * Uses flatMap instead of map to silently discard malformed records without failing the job.
     */
    public static class RawToFlightMapper
            implements org.apache.flink.api.common.functions.FlatMapFunction<String, FlightRecord> {

        private final ObjectMapper mapper;

        public RawToFlightMapper(ObjectMapper mapper) {
            this.mapper = mapper;
        }

        @Override
        public void flatMap(String json, org.apache.flink.util.Collector<FlightRecord> out) {
            try {
                RawFlightRecord raw = mapper.readValue(json, RawFlightRecord.class);
                if (raw != null) {
                    out.collect(new FlightRecord(raw));
                }
            } catch (Exception e) {
                // Swallow parse errors: malformed records are discarded
            }
        }
    }
}
