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

    @Serial
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

    @JsonProperty("DEP_DELAY")
    private Double depDelay;

    @JsonProperty("CANCELLED")
    private Double cancelled;

    @JsonProperty("DIVERTED")
    private Double diverted;

    @JsonProperty("ORIGIN_AIRPORT_ID")
    private Integer originAirportId;

    @JsonProperty("DEST_AIRPORT_ID")
    private Integer destinationAirportId;

    /** Optimized primitive field to cache the calculated event time . */
    private long eventTimeMillis = Long.MIN_VALUE;

    public FlightRecord() {}

    // --- Getters and Setters ---

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

    public Double getDepDelay() { return depDelay != null ? depDelay : 0.0; }
    public void setDepDelay(Double depDelay) { this.depDelay = depDelay; }

    public Double getCancelled() { return cancelled; }
    public void setCancelled(Double cancelled) { this.cancelled = cancelled; }

    public Double getDiverted() { return diverted; }
    public void setDiverted(Double diverted) { this.diverted = diverted; }

    public Integer getOriginAirportId() { return originAirportId; }
    public void setOriginAirportId(Integer originAirportId) { this.originAirportId = originAirportId; }

    public Integer getDestinationAirportId() { return destinationAirportId; }
    public void setDestinationAirportId(Integer destinationAirportId) { this.destinationAirportId = destinationAirportId; }

    public long getEventTimeMillis() { return this.eventTimeMillis; }
    public void setEventTimeMillis(long eventTimeMillis) { this.eventTimeMillis = eventTimeMillis; }

    // --- Logic and Enrichment Helpers ---

    /**
     * Checks if the flight was canceled based on the internal indicator.
     * @return true if canceled, false otherwise
     */
    public boolean isCancelled() {
        return cancelled != null && cancelled == 1.0;
    }

    /**
     * Checks if the flight was diverted based on the internal indicator.
     * @return true if diverted, false otherwise
     */
    public boolean isDiverted() {
        return diverted != null && diverted == 1.0;
    }

    /**
     * Parses the CRS_DEP_TIME field to extract the hour component safely.
     * Validates both hour boundaries and minute boundaries.
     * @return the scheduled hour of departure, or -1 if the field is malformed
     */
    public int getScheduledDepartureHour() {
        if (crsDepTime == null) {
            return -1;
        }

        int hour = (crsDepTime / 100) % 24;
        int minute = crsDepTime % 100;

        if (hour < 0 || minute < 0 || minute > 59) {
            return -1;
        }

        return hour;
    }

    /**
     * Converts the flight's scheduled departure date and time into epoch milliseconds.
     * Handles explicit boundary conditions like a 2400 midnight notation.
     * @return the calculated epoch milliseconds, or Long.MIN_VALUE if validation fails
     */
    public long calculateEpochMillis() {
        if (year == null || month == null || dayOfMonth == null || crsDepTime == null) {
            return Long.MIN_VALUE;
        }
        int hour   = crsDepTime / 100;
        int minute = crsDepTime % 100;

        if (hour == 24 && minute == 0) {
            hour = 23;
            minute = 59;
        }
        if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
            return Long.MIN_VALUE;
        }

        try {
            return LocalDateTime.of(year, month, dayOfMonth, hour, minute)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }

    @Override
    public String toString() {
        return "FlightRecord{" +
                "year=" + year +
                ", month=" + month +
                ", dayOfMonth=" + dayOfMonth +
                ", airline='" + airline + '\'' +
                ", crsDepTime=" + crsDepTime +
                ", depDelay=" + depDelay +
                ", cancelled=" + cancelled +
                ", diverted=" + diverted +
                ", originAirportId=" + originAirportId +
                ", dest='" + destinationAirportId + '\'' +
                '}';
    }

    // --- Embedded Kafka Deserializer ---

    /**
     * Custom deserialization schema to map Kafka raw bytes directly into FlightRecord objects.
     */
    public static class FlightRecordDeserializationSchema implements KafkaRecordDeserializationSchema<FlightRecord> {

        @Serial
        private static final long serialVersionUID = 1L;

        private static final Logger LOG = LoggerFactory.getLogger(FlightRecordDeserializationSchema.class);

        /** Marked transient to avoid serialization errors across distributed worker nodes. */
        private transient ObjectMapper mapper;

        /**
         * Lazily instantiates and provides the ObjectMapper utility instance.
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
         * @param context the Flink runtime context for initialization tasks
         * @throws Exception if initialization fails
         */
        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {
            this.mapper = new ObjectMapper();
        }

        /**
         * Intercepts Kafka byte chunks, deserializes JSON text, and injects performance-cached timestamps.
         * Skips tombstone payloads and discards corrupted JSON messages gracefully.
         * @param record the incoming raw record from Kafka
         * @param out the collector used to output valid FlightRecord entities
         * @throws IOException if low-level stream reading failure occurs
         */
        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<FlightRecord> out) throws IOException {
            if (record.value() == null) {
                return;
            }

            try {
                FlightRecord flight = getMapper().readValue(record.value(), FlightRecord.class);

                long calculatedTime = flight.calculateEpochMillis();
                flight.setEventTimeMillis(calculatedTime);

                out.collect(flight);
            } catch (Exception e) {
                LOG.error("Failed to deserialize JSON record from partition {} at offset {}. Error: {}",
                        record.partition(), record.offset(), e.getMessage());
            }
        }

        /**
         * Informs Flink's type execution subsystem about the explicit output data model structure.
         * @return TypeInformation mapped to the FlightRecord POJO structure
         */
        @Override
        public TypeInformation<FlightRecord> getProducedType() {
            return TypeInformation.of(FlightRecord.class);
        }
    }
}
