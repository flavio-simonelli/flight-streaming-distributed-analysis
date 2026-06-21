package it.uniroma2.sae.query.distribution;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serial;
import java.io.Serializable;

/**
 * Result data model holding delay percentiles for a distinct airline and hour.
 * Serialized as JSON with explicit window type identification.
 */
public class DelayDistributionResult implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @JsonProperty("window_start")
    private long windowStart;

    @JsonProperty("window_end")
    private long windowEnd;

    @JsonProperty("window_type")
    private String windowType; // "1d", "7d", "global"

    @JsonProperty("airline")
    private String airline;

    @JsonProperty("hour")
    private int hour;

    @JsonProperty("count")
    private long count;

    @JsonProperty("min")
    private double min;

    @JsonProperty("p25")
    private double p25;

    @JsonProperty("p50")
    private double p50;

    @JsonProperty("p75")
    private double p75;

    @JsonProperty("p90")
    private double p90;

    @JsonProperty("max")
    private double max;

    public DelayDistributionResult() {}

    public DelayDistributionResult(
            long windowStart, long windowEnd, String windowType,
            String airline, int hour, long count,
            double min, double p25, double p50, double p75, double p90, double max) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.windowType = windowType;
        this.airline = airline;
        this.hour = hour;
        this.count = count;
        this.min = min;
        this.p25 = p25;
        this.p50 = p50;
        this.p75 = p75;
        this.p90 = p90;
        this.max = max;
    }

    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public String getWindowType() { return windowType; }
    public void setWindowType(String windowType) { this.windowType = windowType; }

    public String getAirline() { return airline; }
    public void setAirline(String airline) { this.airline = airline; }

    public int getHour() { return hour; }
    public void setHour(int hour) { this.hour = hour; }

    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }

    public double getMin() { return min; }
    public void setMin(double min) { this.min = min; }

    public double getP25() { return p25; }
    public void setP25(double p25) { this.p25 = p25; }

    public double getP50() { return p50; }
    public void setP50(double p50) { this.p50 = p50; }

    public double getP75() { return p75; }
    public void setP75(double p75) { this.p75 = p75; }

    public double getP90() { return p90; }
    public void setP90(double p90) { this.p90 = p90; }

    public double getMax() { return max; }
    public void setMax(double max) { this.max = max; }
}
