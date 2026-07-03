package it.uniroma2.sae.source;

import it.uniroma2.sae.config.KafkaConfig;
import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Generic SourceBuilder wrapping Flink's KafkaSourceBuilder to enforce project defaults.
 */
public class SourceBuilder {

    private final KafkaSourceBuilder<FlightRecord> builder;

    /**
     * Main constructor initializing the KafkaSource builder with configuration properties.
     * Configures the bootstrap servers, target input topic, consumer group ID,
     * earliest starting offset strategy, and assigns the custom FlightRecord deserializer.
     *
     * @param config the central Kafka configuration container
     */
    public SourceBuilder(KafkaConfig config) {
        this.builder = KafkaSource.<FlightRecord>builder()
            .setBootstrapServers(config.getBootstrapServers())
            .setTopics(config.getInputTopic())
            .setGroupId(config.getGroupId())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new FlightRecord.FlightRecordDeserializationSchema());
    }

    /**
     * Sets bounded offsets for BATCH mode.
     */
    public SourceBuilder setBounded(OffsetsInitializer offsets) {
        this.builder.setBounded(offsets);
        return this;
    }

    /**
     * Builds and returns the configured KafkaSource instance.
     */
    public KafkaSource<FlightRecord> build() {
        return this.builder.build();
    }
}
