package it.uniroma2.sae.sink;

import it.uniroma2.sae.config.KafkaConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Simplified SinkBuilder using composition to wrap Flink's KafkaSinkBuilder with project defaults.
 * Direct inheritance is not possible due to package-private constructors in Flink.
 */
public class SinkBuilder<T> {

    private final KafkaSinkBuilder<T> builder;

    public SinkBuilder(KafkaConfig config) {
        this.builder = KafkaSink.<T>builder()
            .setBootstrapServers(config.getHost() + ":" + config.getInternalPort())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
    }

    public static SinkBuilder<String> defaultStringSink(KafkaConfig config) {
        SinkBuilder<String> sb = new SinkBuilder<>(config);
        sb.builder.setRecordSerializer(new RecordSerializer(config.getSinkTopic()));
        return sb;
    }

    public SinkBuilder<T> withRecordSerializer(KafkaRecordSerializationSchema<T> serializer) {
        this.builder.setRecordSerializer(serializer);
        return this;
    }

    public KafkaSink<T> build() {
        return this.builder.build();
    }

    public static class RecordSerializer implements KafkaRecordSerializationSchema<String>, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        public RecordSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
            return new ProducerRecord<>(
                    this.topic,
                    null,
                    null,
                    null,
                    element.getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}
