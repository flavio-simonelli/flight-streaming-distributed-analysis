package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System integration test using dynamic configuration and batch mode.
 */
public class SystemIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SystemIntegrationTest.class);
    private static ApplicationConfig config;

    @BeforeAll
    public static void setup() {
        config = ApplicationConfig.load("application.yaml");
        LOG.info("Configuration loaded: Flink={}:{}, Kafka={}:{}", 
                config.getFlink().getHost(), config.getFlink().getPort(),
                config.getKafka().getHost(), config.getKafka().getPort());
    }

    private boolean isReachable(String host, int port) {
        LOG.info("Checking connectivity to {}:{}...", host, port);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 3000);
            return true;
        } catch (IOException e) {
            LOG.warn("Host {}:{} is not reachable: {}", host, port, e.getMessage());
            return false;
        }
    }

    @Test
    public void testDelayAverage() throws Exception {
        String flinkHost = config.getFlink().getHost();
        int flinkPort = config.getFlink().getPort();
        String kafkaHost = config.getKafka().getHost();
        int kafkaPort = config.getKafka().getExternalPort();

        // Ensure cluster and kafka are UP using values from ApplicationConfig
        Assumptions.assumeTrue(isReachable(flinkHost, flinkPort), "Flink JobManager MUST be reachable at " + flinkHost + ":" + flinkPort);
        Assumptions.assumeTrue(isReachable(kafkaHost, kafkaPort), "Kafka MUST be reachable at " + kafkaHost + ":" + kafkaPort);

        // createRemoteEnvironment is AutoCloseable in Flink 1.15+
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                flinkHost,
                flinkPort,
                "target/flight-analysis-1.0.jar",
                "target/flight-analysis-1.0-tests.jar"
        )) {
            LOG.info("Initializing and submitting BATCH job to cluster {}...", flinkHost);
            FlightDelayJob job = new FlightDelayJob(config);
            job.defineJob(env);
            
            env.execute("Flight Delay Average System Test");
            LOG.info("Flink BATCH execution finished.");

        } catch (Exception e) {
            LOG.error("Flink job submission/execution failed: {}", e.getMessage(), e);
            throw e;
        }

        // Final verification by consuming from Kafka results topic
        verifyResults(kafkaHost, kafkaPort);
    }

    private void verifyResults(String host, int port) {
        LOG.info("Consuming results from Kafka at {}:{} for validation...", host, port);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(config.getKafka().getSinkTopic()));
            
            double lastValue = -1.0;
            long deadline = System.currentTimeMillis() + 15000;
            
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    lastValue = Double.parseDouble(record.value());
                }
                if (lastValue > 0) break;
            }

            assertTrue(lastValue > 0, "No results found in Kafka topic " + config.getKafka().getSinkTopic());
            LOG.debug("Validation: Expected=12.4, Actual={}", lastValue);
            assertEquals(12.4, lastValue, 0.02, "Final average delay mismatch!");
        }
    }
}
