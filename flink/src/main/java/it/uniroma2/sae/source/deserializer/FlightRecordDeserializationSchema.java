package it.uniroma2.sae.source.deserializer;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * Custom deserialization schema to map Kafka raw bytes directly into FlightRecord objects.
 */
public class FlightRecordDeserializationSchema implements KafkaRecordDeserializationSchema<FlightRecord> {
    
    private static final long serialVersionUID = 1L;
    
    private static final Logger LOG = LoggerFactory.getLogger(FlightRecordDeserializationSchema.class);

    // Marked transient to prevent serialization issues across the distributed Flink cluster
    private transient ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        // Initialize the ObjectMapper inside the open method, which executes on the TaskManagers
        this.mapper = new ObjectMapper();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<FlightRecord> out) throws IOException {
        // Skip tombstone records (empty messages often used in Kafka to signal deletion)
        if (record.value() == null) {
            return; 
        }
        
        try {
            // Direct byte-to-object deserialization via Jackson
            FlightRecord flight = mapper.readValue(record.value(), FlightRecord.class);
            
            // Emit the deserialized object into the Flink stream
            out.collect(flight);
        } catch (Exception e) {
            // Fault tolerance: log corrupt JSON payloads and skip them to keep the streaming pipeline alive
            LOG.error("Failed to deserialize JSON record from partition " + record.partition() 
                + " at offset " + record.offset() + ". Error: " + e.getMessage());
        }
    }

    @Override
    public TypeInformation<FlightRecord> getProducedType() {
        // Informs Flink's type system about the explicit output data type
        return TypeInformation.of(FlightRecord.class);
    }
}