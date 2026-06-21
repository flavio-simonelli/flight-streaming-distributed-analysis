package it.uniroma2.sae.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Unified data model representing a flight record for Flink streaming.
 * Integrates raw JSON deserialization annotations and clean helper methods.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlightRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("YEAR")
    private Integer year;

    @JsonProperty("MONTH")
    private Integer month;

    @JsonProperty("DAY_OF_MONTH")
    private Integer dayOfMonth;

    @JsonProperty("OP_UNIQUE_CARRIER")
    private String airline;

    @JsonProperty("CRS_DEP_TIME")
    private Integer crsDepTime;

    @JsonProperty("DEP_TIME")
    private Double depTime;

    @JsonProperty("DEP_DELAY")
    private Double depDelay;

    @JsonProperty("CANCELLED")
    private Double cancelled;

    @JsonProperty("DIVERTED")
    private Double diverted;

    @JsonProperty("ORIGIN_AIRPORT_ID")
    private Integer originAirportId;

    @JsonProperty("DEST")
    private String dest;

    // Default constructor required for Jackson and Flink POJO serialization
    public FlightRecord() {}

    // Getters and Setters

    public Integer getYear() { return year; }
    public void setYear(Integer year) { this.year = year; }

    public Integer getMonth() { return month; }
    public void setMonth(Integer month) { this.month = month; }

    public Integer getDayOfMonth() { return dayOfMonth; }
    public void setDayOfMonth(Integer dayOfMonth) { this.dayOfMonth = dayOfMonth; }

    public String getAirline() { return airline; }
    public void setAirline(String airline) { this.airline = airline; }

    public Integer getCrsDepTime() { return crsDepTime; }
    public void setCrsDepTime(Integer crsDepTime) { this.crsDepTime = crsDepTime; }

    public Double getDepTime() { return depTime; }
    public void setDepTime(Double depTime) { this.depTime = depTime; }

    public Double getDepDelay() { return depDelay != null ? depDelay : 0.0; }
    public void setDepDelay(Double depDelay) { this.depDelay = depDelay; }

    public Double getCancelled() { return cancelled; }
    public void setCancelled(Double cancelled) { this.cancelled = cancelled; }

    public Double getDiverted() { return diverted; }
    public void setDiverted(Double diverted) { this.diverted = diverted; }

    public Integer getOriginAirportId() { return originAirportId; }
    public void setOriginAirportId(Integer originAirportId) { this.originAirportId = originAirportId; }

    public String getDest() { return dest; }
    public void setDest(String dest) { this.dest = dest; }

    // Logical wrappers

    public boolean isCancelled() {
        return cancelled != null && cancelled == 1.0;
    }

    public boolean isDiverted() {
        return diverted != null && diverted == 1.0;
    }

    /**
     * Converts the flight's scheduled departure date/time (CRS_DEP_TIME) to epoch milliseconds.
     * Used by Flink's TimestampAssigner to assign Event Time to each record.
     * CRS_DEP_TIME format: HHMM (e.g. 1530 = 15:30)
     */
    public long getEventTimeMillis() {
        if (year == null || month == null || dayOfMonth == null || crsDepTime == null) {
            return Long.MIN_VALUE;
        }
        int hour   = crsDepTime / 100;
        int minute = crsDepTime % 100;
        return LocalDateTime.of(year, month, dayOfMonth, hour, minute)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
    }

    @Override
    public String toString() {
        return "FlightRecord{" +
                "year=" + year +
                ", month=" + month +
                ", dayOfMonth=" + dayOfMonth +
                ", airline='" + airline + '\'' +
                ", crsDepTime=" + crsDepTime +
                ", depTime=" + depTime +
                ", depDelay=" + depDelay +
                ", cancelled=" + cancelled +
                ", diverted=" + diverted +
                ", originAirportId=" + originAirportId +
                ", dest='" + dest + '\'' +
                '}';
    }

    /**
     * Custom deserialization schema to map Kafka raw bytes directly into FlightRecord objects.
     */
    public static class FlightRecordDeserializationSchema implements KafkaRecordDeserializationSchema<FlightRecord> {

        @Serial
        private static final long serialVersionUID = 1L;

        private static final Logger LOG = LoggerFactory.getLogger(FlightRecordDeserializationSchema.class);

        // Marked transient to prevent serialization issues across the distributed Flink cluster
        private transient ObjectMapper mapper;

        private ObjectMapper getMapper() {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper;
        }

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
                // Deserialize directly to FlightRecord POJO
                FlightRecord flight = getMapper().readValue(record.value(), FlightRecord.class);

                // Emit the deserialized object into the Flink stream
                out.collect(flight);
            } catch (Exception e) {
                // Fault tolerance: log corrupt JSON payloads and skip them to keep the streaming pipeline alive
                LOG.error("Failed to deserialize JSON record from partition {} at offset {}. Error: {}", record.partition(), record.offset(), e.getMessage());
            }
        }

        @Override
        public TypeInformation<FlightRecord> getProducedType() {
            // Informs Flink's type system about the explicit output data type
            return TypeInformation.of(FlightRecord.class);
        }
    }
}
