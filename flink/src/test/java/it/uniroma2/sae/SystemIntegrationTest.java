package it.uniroma2.sae;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;

/**
 * System test for calculating average departure delays and saving results to InfluxDB.
 */
public class SystemIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SystemIntegrationTest.class);
    private static ApplicationConfig config;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeAll
    public static void setup() {
        config = ApplicationConfig.load("application.yaml");
    }

    private boolean isReachable(String url) {
        url = url.replace("https://", "").replace("http://", "");
        String[] split = url.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        return isReachable(host, port);
    }

    private boolean isReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    public void testDelayAverage() throws Exception {
        String kafkaHost = config.getKafka().getHost();
        int kafkaPort = config.getKafka().getPort();
        String influxUrl = config.getInfluxdb().getUrl();
        String flinkUrl = config.getFlink().getHost();
        int flinkPort = config.getFlink().getPort();

        // Check infrastructure using localhost
        Assumptions.assumeTrue(isReachable(kafkaHost, kafkaPort), "Kafka is required");
        Assumptions.assumeTrue(isReachable(influxUrl), "InfluxDB is required");
        Assumptions.assumeTrue(isReachable(flinkUrl, flinkPort), "Flink is required");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaHost + ":" + kafkaPort)
                .setTopics(config.getKafka().getTopic())
                .setGroupId(config.getKafka().getGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        LOG.info("Starting delay average processing...");

        String token = config.getInfluxdb().getToken();
        String organization = config.getInfluxdb().getOrg();
        String bucketName = config.getInfluxdb().getBucket();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> {
                    try {
                        return OBJECT_MAPPER.readValue(json, FlightRecord.class);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(record -> record != null && record.getDepDelay() != null)
                .map(record -> new Tuple2<>(record.getDepDelay(), 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Double, Long>>(){}))
                .keyBy(t -> "global")
                .reduce((t1, t2) -> new Tuple2<>(t1.f0 + t2.f0, t1.f1 + t2.f1))
                .map(new MapFunction<Tuple2<Double, Long>, Double>() {
                    @Override
                    public Double map(Tuple2<Double, Long> result) {
                        double avg = result.f0 / result.f1;
                        LOG.info("Calculated Average Delay: {}", avg);
                        return avg;
                    }
                })
                .addSink(new InfluxDBCustomSink(influxUrl, token, organization, bucketName));

        env.execute("Flight Delay Average System Test");
        LOG.info("System test completed.");
    }

    /**
     * Custom Sink using InfluxDB 2 Java client with InfluxDB 3 compatibility.
     */
    public static class InfluxDBCustomSink extends RichSinkFunction<Double> {
        private final String url;
        private final String token;
        private final String org;
        private final String bucket;
        private transient InfluxDBClient client;

        public InfluxDBCustomSink(String url, String token, String org, String bucket) {
            this.url = url;
            this.token = token;
            this.org = org;
            this.bucket = bucket;
        }

        @Override
        public void open(OpenContext context) {
            LOG.info("Opening InfluxDB connection to {} (org={}, bucket={})", url, org, bucket);
            
            // InfluxDB 3-core requires 'Bearer' prefix and often ignores 'org' (can use '-').
            OkHttpClient.Builder httpClient = new OkHttpClient.Builder()
                    .addInterceptor(chain -> {
                        Request authenticatedRequest = chain.request().newBuilder()
                                .header("Authorization", "Bearer " + token)
                                .build();
                        return chain.proceed(authenticatedRequest);
                    });

            InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                    .url(url)
                    .okHttpClient(httpClient)
                    .org(org)
                    .bucket(bucket)
                    .build();

            client = InfluxDBClientFactory.create(options);
        }

        @Override
        public void invoke(Double avgDelay, Context context) {
            WriteApiBlocking writeApi = client.getWriteApiBlocking();
            Point point = Point.measurement("flight_metrics")
                    .addTag("source", "flink_system_test")
                    .addField("avg_dep_delay", avgDelay)
                    .time(Instant.now(), WritePrecision.NS);
            writeApi.writePoint(point);
            LOG.info("Successfully wrote average delay {} to InfluxDB", avgDelay);
        }

        @Override
        public void close() {
            if (client != null) {
                client.close();
            }
        }
    }
}
