package it.uniroma2.sae.source;

import it.uniroma2.sae.config.KafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Simplified SourceBuilder using composition to wrap Flink's KafkaSourceBuilder with project defaults.
 * Direct inheritance is not possible due to package-private constructors in Flink.
 */
public class SourceBuilder {

    private final KafkaSourceBuilder<String> builder;

    public SourceBuilder(KafkaConfig config) {
        this.builder = KafkaSource.<String>builder()
            .setBootstrapServers(config.getBootstrapServers())
            .setTopics(config.getInputTopic())
            .setGroupId(config.getGroupId())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema());
    }

    /**
     * Sets bounded offsets for BATCH mode.
     */
    public SourceBuilder setBounded(OffsetsInitializer offsets) {
        this.builder.setBounded(offsets);
        return this;
    }

    public KafkaSource<String> build() {
        return this.builder.build();
    }
}
