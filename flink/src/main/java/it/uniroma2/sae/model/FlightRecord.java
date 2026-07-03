package it.uniroma2.sae.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Unified data model representing a flight record for Flink streaming.
 * Follows strict Java Bean POJO specifications for optimized Flink serialization.
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

    /** Optimized primitive field to cache the calculated event time. */
    private long eventTimeMillis = Long.MIN_VALUE;

    private long systemIngestionTime = 0L;

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

    public long getSystemIngestionTime() { return this.systemIngestionTime; }
    public void setSystemIngestionTime(long systemIngestionTime) { this.systemIngestionTime = systemIngestionTime; }

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
}
