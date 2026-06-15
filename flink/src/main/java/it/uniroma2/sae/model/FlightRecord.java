package it.uniroma2.sae.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data model representing a flight record from Kafka.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlightRecord {

    @JsonProperty("YEAR")
    private int year;

    @JsonProperty("MONTH")
    private int month;

    @JsonProperty("DAY_OF_MONTH")
    private int dayOfMonth;

    @JsonProperty("OP_UNIQUE_CARRIER")
    private String carrier;

    @JsonProperty("DEP_DELAY")
    private Double depDelay;

    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    public int getMonth() { return month; }
    public void setMonth(int month) { this.month = month; }

    public int getDayOfMonth() { return dayOfMonth; }
    public void setDayOfMonth(int dayOfMonth) { this.dayOfMonth = dayOfMonth; }

    public String getCarrier() { return carrier; }
    public void setCarrier(String carrier) { this.carrier = carrier; }

    public Double getDepDelay() { return depDelay; }
    public void setDepDelay(Double depDelay) { this.depDelay = depDelay; }
}
