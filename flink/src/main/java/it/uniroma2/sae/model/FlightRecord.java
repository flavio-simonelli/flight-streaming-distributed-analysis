package it.uniroma2.sae.model;

import java.io.Serializable;

/**
 * Clean data model representing a flight record for Flink processing.
 */
public class FlightRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    // Campi temporali e identificativi nativi
    private final int year;
    private final int month;
    private final int dayOfMonth;
    private final String airline;
    private final int crsDepTime;

    // Metriche di performance convertite in primitivi (sicure contro i NullPointerException)
    private final double depDelay;
    private final double arrDelay;

    // Flag di stato convertiti nel tipo booleano logico appropriato
    private final boolean cancelled;
    private final boolean diverted;

    // Cause del ritardo
    private final double carrierDelay;
    private final double weatherDelay;
    private final double nasDelay;
    private final double securityDelay;
    private final double lateAircraftDelay;

    /**
     * Constructs a clean FlightRecord from a RawFlightRecord.
     * Performs missing value handling and type conversions.
     */
    public FlightRecord(RawFlightRecord raw) {
        this.year = raw.getYear();
        this.month = raw.getMonth();
        this.dayOfMonth = raw.getDayOfMonth();
        this.airline = raw.getCarrier(); // Mappa OP_UNIQUE_CARRIER direttamente su airline
        this.crsDepTime = raw.getCrsDepTime();

        // 1. GESTIONE VALORI MANCANTI: Trattati come 0.0
        this.depDelay = raw.getDepDelay() != null ? raw.getDepDelay() : 0.0;
        this.arrDelay = raw.getArrDelay() != null ? raw.getArrDelay() : 0.0;

        // 2. CONVERSIONE DEI TIPI DI STATO: Trasformazione da Double (1.0 / 0.0) a boolean
        this.cancelled = raw.getCancelled() != null && raw.getCancelled() == 1.0;
        this.diverted = raw.getDiverted() != null && raw.getDiverted() == 1.0;

        // 3. PULIZIA DELLE CAUSE SPECIFICHE DI RITARDO
        this.carrierDelay = raw.getCarrierDelay() != null ? raw.getCarrierDelay() : 0.0;
        this.weatherDelay = raw.getWeatherDelay() != null ? raw.getWeatherDelay() : 0.0;
        this.nasDelay = raw.getNasDelay() != null ? raw.getNasDelay() : 0.0;
        this.securityDelay = raw.getSecurityDelay() != null ? raw.getSecurityDelay() : 0.0;
        this.lateAircraftDelay = raw.getLateAircraftDelay() != null ? raw.getLateAircraftDelay() : 0.0;
    }

    // Getters

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDayOfMonth() {
        return dayOfMonth;
    }

    public String getAirline() {
        return airline;
    }

    public int getCrsDepTime() {
        return crsDepTime;
    }

    public double getDepDelay() {
        return depDelay;
    }

    public double getArrDelay() {
        return arrDelay;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean getCancelled() {
        return cancelled;
    }

    public boolean isDiverted() {
        return diverted;
    }

    public boolean getDiverted() {
        return diverted;
    }

    public double getCarrierDelay() {
        return carrierDelay;
    }

    public double getWeatherDelay() {
        return weatherDelay;
    }

    public double getNasDelay() {
        return nasDelay;
    }

    public double getSecurityDelay() {
        return securityDelay;
    }

    public double getLateAircraftDelay() {
        return lateAircraftDelay;
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
                ", arrDelay=" + arrDelay +
                ", cancelled=" + cancelled +
                ", diverted=" + diverted +
                ", carrierDelay=" + carrierDelay +
                ", weatherDelay=" + weatherDelay +
                ", nasDelay=" + nasDelay +
                ", securityDelay=" + securityDelay +
                ", lateAircraftDelay=" + lateAircraftDelay +
                '}';
    }
}
