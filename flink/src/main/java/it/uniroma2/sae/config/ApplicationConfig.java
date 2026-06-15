package it.uniroma2.sae.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStream;

/**
 * Application configuration loaded from a YAML file.
 */
public class ApplicationConfig {

    private KafkaConfig kafka;
    private FlinkConfig flink;
    private InfluxDBConfig influxdb;

    public KafkaConfig getKafka() { return kafka; }
    public void setKafka(KafkaConfig kafka) { this.kafka = kafka; }

    public FlinkConfig getFlink() { return flink; }
    public void setFlink(FlinkConfig flink) { this.flink = flink; }

    public InfluxDBConfig getInfluxdb() { return influxdb; }
    public void setInfluxdb(InfluxDBConfig influxdb) { this.influxdb = influxdb; }

    /**
     * Loads the configuration from the specified YAML file in the classpath.
     *
     * @param fileName the name of the YAML file
     * @return the loaded ApplicationConfig instance
     */
    public static ApplicationConfig load(String fileName) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try (InputStream is = ApplicationConfig.class.getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                throw new IllegalArgumentException("File not found: " + fileName);
            }
            return mapper.readValue(is, ApplicationConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }
}
