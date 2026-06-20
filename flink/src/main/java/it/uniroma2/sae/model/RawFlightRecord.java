package it.uniroma2.sae.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raw flight record model as read from Kafka.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawFlightRecord {

    @JsonProperty("YEAR")
    private int year;

    @JsonProperty("MONTH")
    private int month;

    @JsonProperty("DAY_OF_MONTH")
    private int dayOfMonth;

    @JsonProperty("OP_UNIQUE_CARRIER")
    private String carrier;

    @JsonProperty("CRS_DEP_TIME")
    private int crsDepTime;

    @JsonProperty("DEP_DELAY")
    private Double depDelay;

    @JsonProperty("ARR_DELAY")
    private Double arrDelay;

    @JsonProperty("CANCELLED")
    private Double cancelled;

    @JsonProperty("DIVERTED")
    private Double diverted;

    @JsonProperty("CARRIER_DELAY")
    private Double carrierDelay;

    @JsonProperty("WEATHER_DELAY")
    private Double weatherDelay;

    @JsonProperty("NAS_DELAY")
    private Double nasDelay;

    @JsonProperty("SECURITY_DELAY")
    private Double securityDelay;

    @JsonProperty("LATE_AIRCRAFT_DELAY")
    private Double lateAircraftDelay;

    @JsonProperty("ORIGIN_AIRPORT_ID")
    private Integer originAirportId;

    @JsonProperty("DEST")
    private String dest;

    // Getters and Setters

    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    public int getMonth() { return month; }
    public void setMonth(int month) { this.month = month; }

    public int getDayOfMonth() { return dayOfMonth; }
    public void setDayOfMonth(int dayOfMonth) { this.dayOfMonth = dayOfMonth; }

    public String getCarrier() { return carrier; }
    public void setCarrier(String carrier) { this.carrier = carrier; }

    public int getCrsDepTime() { return crsDepTime; }
    public void setCrsDepTime(int crsDepTime) { this.crsDepTime = crsDepTime; }

    public Double getDepDelay() { return depDelay; }
    public void setDepDelay(Double depDelay) { this.depDelay = depDelay; }

    public Double getArrDelay() { return arrDelay; }
    public void setArrDelay(Double arrDelay) { this.arrDelay = arrDelay; }

    public Double getCancelled() { return cancelled; }
    public void setCancelled(Double cancelled) { this.cancelled = cancelled; }

    public Double getDiverted() { return diverted; }
    public void setDiverted(Double diverted) { this.diverted = diverted; }

    public Double getCarrierDelay() { return carrierDelay; }
    public void setCarrierDelay(Double carrierDelay) { this.carrierDelay = carrierDelay; }

    public Double getWeatherDelay() { return weatherDelay; }
    public void setWeatherDelay(Double weatherDelay) { this.weatherDelay = weatherDelay; }

    public Double getNasDelay() { return nasDelay; }
    public void setNasDelay(Double nasDelay) { this.nasDelay = nasDelay; }

    public Double getSecurityDelay() { return securityDelay; }
    public void setSecurityDelay(Double securityDelay) { this.securityDelay = securityDelay; }

    public Double getLateAircraftDelay() { return lateAircraftDelay; }
    public void setLateAircraftDelay(Double lateAircraftDelay) { this.lateAircraftDelay = lateAircraftDelay; }

    public Integer getOriginAirportId() { return originAirportId; }
    public void setOriginAirportId(Integer originAirportId) { this.originAirportId = originAirportId; }

    public String getDest() { return dest; }
    public void setDest(String dest) { this.dest = dest; }

    @Override
    public String toString() {
        return "RawFlightRecord{" +
                "year=" + year +
                ", month=" + month +
                ", dayOfMonth=" + dayOfMonth +
                ", carrier='" + carrier + '\'' +
                ", crsDepTime=" + crsDepTime +
                ", depDelay=" + depDelay +
                ", arrDelay=" + arrDelay +
                ", cancelled=" + cancelled +
                ", diverted=" + diverted +
                ", carrierDelay=" + carrierDelay +
                ", weatherDelay=" + weatherDelay +
                ", nasDelay=" + nasDelay +
                ", securityDelay=" + securityDelay +
                ", lateAircraftDelay=" + lateAircraftDelay +
                ", originAirportId=" + originAirportId +
                ", dest='" + dest + '\'' +
                '}';
    }
}
