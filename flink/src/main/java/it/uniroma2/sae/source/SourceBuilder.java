package it.uniroma2.sae.source;

import it.uniroma2.sae.config.KafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

/**
 * Generic SourceBuilder wrapping Flink's KafkaSourceBuilder to enforce project defaults.
 * @param <T> The type of record produced by this source.
 */
public class SourceBuilder<T> {

    private final KafkaSourceBuilder<T> builder;

    /**
     * Main constructor requiring an explicit deserialization schema.
     */
    public SourceBuilder(KafkaConfig config, KafkaRecordDeserializationSchema<T> deserializer) {
        this.builder = KafkaSource.<T>builder()
            .setBootstrapServers(config.getBootstrapServers())
            .setTopics(config.getInputTopic())
            .setGroupId(config.getGroupId())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(deserializer); // Attaches the user-defined deserializer
    }

    /**
     * Static factory method to maintain backward compatibility with legacy String sources.
     */
    public static SourceBuilder<String> createStringSource(KafkaConfig config) {
        return new SourceBuilder<>(config, KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()));
    }

    /**
     * Sets bounded offsets for BATCH mode.
     */
    public SourceBuilder<T> setBounded(OffsetsInitializer offsets) {
        this.builder.setBounded(offsets);
        return this;
    }

    /**
     * Builds and returns the configured KafkaSource instance.
     */
    public KafkaSource<T> build() {
        return this.builder.build();
    }
}
