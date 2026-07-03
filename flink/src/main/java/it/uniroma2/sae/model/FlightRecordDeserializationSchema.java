package it.uniroma2.sae.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serial;

/**
 * Custom deserialization schema to map Kafka raw JSON bytes directly into FlightRecord POJO instances.
 * Separated from the core FlightRecord class to preserve POJO purity and decouple the model from Flink APIs.
 */
public class FlightRecordDeserializationSchema implements KafkaRecordDeserializationSchema<FlightRecord> {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlightRecordDeserializationSchema.class);

    /** Marked transient to avoid serialization errors across distributed worker nodes. */
    private transient ObjectMapper mapper;

    /**
     * Lazily instantiates and provides the ObjectMapper utility instance.
     *
     * @return the active ObjectMapper instance
     */
    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    /**
     * Lifecycle initialization hook called when the parallel operator runs on TaskManagers.
     *
     * @param context the Flink runtime context for initialization tasks
     */
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.mapper = new ObjectMapper();
    }

    /**
     * Intercepts Kafka byte chunks, deserializes JSON text, and injects performance-cached timestamps.
     * Skips tombstone payloads and discards corrupted JSON messages gracefully.
     *
     * @param record the incoming raw record from Kafka
     * @param out the collector used to output valid FlightRecord entities
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<FlightRecord> out) {
        if (record.value() == null) {
            return;
        }

        try {
            FlightRecord flight = getMapper().readValue(record.value(), FlightRecord.class);

            long calculatedTime = flight.calculateEpochMillis();
            flight.setEventTimeMillis(calculatedTime);
            flight.setSystemIngestionTime(System.currentTimeMillis());

            out.collect(flight);
        } catch (Exception e) {
            LOG.error("Failed to deserialize JSON record from partition {} at offset {}. Error: {}",
                    record.partition(), record.offset(), e.getMessage());
        }
    }

    /**
     * Informs Flink's type execution subsystem about the explicit output data model structure.
     *
     * @return TypeInformation mapped to the FlightRecord POJO structure
     */
    @Override
    public TypeInformation<FlightRecord> getProducedType() {
        return TypeInformation.of(FlightRecord.class);
    }
}
