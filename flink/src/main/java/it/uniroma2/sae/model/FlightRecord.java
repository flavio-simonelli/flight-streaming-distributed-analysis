package it.uniroma2.sae.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Clean data model representing a flight record for Flink processing.
 */
public class FlightRecord implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    // Campi temporali e identificativi nativi
    private int year;
    private int month;
    private int dayOfMonth;
    private String airline;
    private int crsDepTime;

    // Metriche di performance convertite in primitivi (sicure contro i NullPointerException)
    private double depDelay;
    private double arrDelay;

    // Flag di stato convertiti nel tipo booleano logico appropriato
    private boolean cancelled;
    private boolean diverted;

    // Cause del ritardo
    private double carrierDelay;
    private double weatherDelay;
    private double nasDelay;
    private double securityDelay;
    private double lateAircraftDelay;

    private int originAirportId;
    private String dest;

    /**
     * No-argument constructor required by Flink's POJO serializer.
     */
    public FlightRecord() {}

    /**
     * Constructs a clean FlightRecord from a RawFlightRecord.
     * Performs missing value handling and type conversions.
     */
    public FlightRecord(RawFlightRecord raw) {
        this.year       = raw.getYear();
        this.month      = raw.getMonth();
        this.dayOfMonth = raw.getDayOfMonth();
        this.airline    = raw.getCarrier(); // Mappa OP_UNIQUE_CARRIER direttamente su airline
        this.crsDepTime = raw.getCrsDepTime();

        // 1. GESTIONE VALORI MANCANTI: Trattati come 0.0
        this.depDelay = raw.getDepDelay() != null ? raw.getDepDelay() : 0.0;
        this.arrDelay = raw.getArrDelay() != null ? raw.getArrDelay() : 0.0;

        // 2. CONVERSIONE DEI TIPI DI STATO: Trasformazione da Double (1.0 / 0.0) a boolean
        this.cancelled = raw.getCancelled() != null && raw.getCancelled() == 1.0;
        this.diverted  = raw.getDiverted()  != null && raw.getDiverted()  == 1.0;

        // 3. PULIZIA DELLE CAUSE SPECIFICHE DI RITARDO
        this.carrierDelay      = raw.getCarrierDelay()      != null ? raw.getCarrierDelay()      : 0.0;
        this.weatherDelay      = raw.getWeatherDelay()      != null ? raw.getWeatherDelay()      : 0.0;
        this.nasDelay          = raw.getNasDelay()          != null ? raw.getNasDelay()          : 0.0;
        this.securityDelay     = raw.getSecurityDelay()     != null ? raw.getSecurityDelay()     : 0.0;
        this.lateAircraftDelay = raw.getLateAircraftDelay() != null ? raw.getLateAircraftDelay() : 0.0;

        this.originAirportId   = raw.getOriginAirportId()   != null ? raw.getOriginAirportId()   : 0;
        this.dest              = raw.getDest()              != null ? raw.getDest()              : "UNKNOWN";
    }

    /**
     * Converts the flight's scheduled departure date/time (CRS_DEP_TIME) to epoch milliseconds.
     * Used by Flink's TimestampAssigner to assign Event Time to each record.
     * CRS_DEP_TIME format: HHMM (e.g. 1530 = 15:30)
     */
    public long getEventTimeMillis() {
        if (year == 0 || month == 0 || dayOfMonth == 0) {
            return Long.MIN_VALUE; // invalid timestamp: Flink will treat as out-of-order
        }
        int hour   = crsDepTime / 100;
        int minute = crsDepTime % 100;
        return LocalDateTime.of(year, month, dayOfMonth, hour, minute)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
    }

    // Getters

    public int getYear()           { return year; }
    public int getMonth()          { return month; }
    public int getDayOfMonth()     { return dayOfMonth; }
    public String getAirline()     { return airline; }
    public int getCrsDepTime()     { return crsDepTime; }
    public double getDepDelay()    { return depDelay; }
    public double getArrDelay()    { return arrDelay; }
    public boolean isCancelled()   { return cancelled; }
    public boolean getCancelled()  { return cancelled; }
    public boolean isDiverted()    { return diverted; }
    public boolean getDiverted()   { return diverted; }
    public double getCarrierDelay()      { return carrierDelay; }
    public double getWeatherDelay()      { return weatherDelay; }
    public double getNasDelay()          { return nasDelay; }
    public double getSecurityDelay()     { return securityDelay; }
    public double getLateAircraftDelay() { return lateAircraftDelay; }
    public int getOriginAirportId()      { return originAirportId; }
    public String getDest()              { return dest; }

    // Setters required by Flink's POJO serializer

    public void setYear(int year) { this.year = year; }
    public void setMonth(int month) { this.month = month; }
    public void setDayOfMonth(int dayOfMonth) { this.dayOfMonth = dayOfMonth; }
    public void setAirline(String airline) { this.airline = airline; }
    public void setCrsDepTime(int crsDepTime) { this.crsDepTime = crsDepTime; }
    public void setDepDelay(double depDelay) { this.depDelay = depDelay; }
    public void setArrDelay(double arrDelay) { this.arrDelay = arrDelay; }
    public void setCancelled(boolean cancelled) { this.cancelled = cancelled; }
    public void setDiverted(boolean diverted) { this.diverted = diverted; }
    public void setCarrierDelay(double carrierDelay) { this.carrierDelay = carrierDelay; }
    public void setWeatherDelay(double weatherDelay) { this.weatherDelay = weatherDelay; }
    public void setNasDelay(double nasDelay) { this.nasDelay = nasDelay; }
    public void setSecurityDelay(double securityDelay) { this.securityDelay = securityDelay; }
    public void setLateAircraftDelay(double lateAircraftDelay) { this.lateAircraftDelay = lateAircraftDelay; }
    public void setOriginAirportId(int originAirportId) { this.originAirportId = originAirportId; }
    public void setDest(String dest) { this.dest = dest; }

    @Override
    public String toString() {
        return "FlightRecord{" +
                "year=" + year +
                ", month=" + month +
                ", dayOfMonth=" + dayOfMonth +
                ", airline='" + airline + '\'' +
                ", crsDepTime=" + crsDepTime +
                ", depDelay=" + depDelay +
                ", arrDelay=" + arrDelay +
                ", cancelled=" + cancelled +
                ", diverted=" + diverted +
                '}';
    }

    /**
     * FlatMapFunction that deserializes a raw JSON string from Kafka into a clean FlightRecord.
     * Uses flatMap instead of map to silently discard malformed records without failing the job.
     */
    public static class RawToFlightMapper implements FlatMapFunction<String, FlightRecord> {

        private final ObjectMapper mapper;

        public RawToFlightMapper(ObjectMapper mapper) {
            this.mapper = mapper;
        }

        @Override
        public void flatMap(String json, Collector<FlightRecord> out) {
            try {
                RawFlightRecord raw = mapper.readValue(json, RawFlightRecord.class);
                if (raw != null) {
                    out.collect(new FlightRecord(raw));
                }
            } catch (Exception e) {
                // Swallow parse errors: malformed records are discarded
            }
        }
    }
}
