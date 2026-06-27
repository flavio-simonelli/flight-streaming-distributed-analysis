package it.uniroma2.sae.config;

/**
 * Flink-specific configuration properties.
 */
public class FlinkConfig {
    private String host;
    private int port;
    private int watermarkDelayMinutes = 10;
    private int allowedLatenessMinutes = 5;
    private Integer allowedLatenessQ1Minutes;
    private Integer allowedLatenessQ2_1hMinutes;
    private Integer allowedLatenessQ2_6hMinutes;
    private Integer allowedLatenessQ2_globalMinutes;
    private Integer allowedLatenessQ3_1dMinutes;
    private Integer allowedLatenessQ3_7dMinutes;
    private int watermarkIdlenessMinutes = 2;
    private int globalWindowTriggerHours = 1;
    private int maxParallelism = 6;

    /** Checkpoint storage backend and behavior configuration. */
    private CheckpointConfig checkpoint;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public int getGlobalWindowTriggerHours() { return globalWindowTriggerHours; }
    public void setGlobalWindowTriggerHours(int globalWindowTriggerHours) { this.globalWindowTriggerHours = globalWindowTriggerHours; }

    public int getWatermarkDelayMinutes() { return watermarkDelayMinutes; }
    public void setWatermarkDelayMinutes(int watermarkDelayMinutes) { this.watermarkDelayMinutes = watermarkDelayMinutes; }

    public int getAllowedLatenessMinutes() { return allowedLatenessMinutes; }
    public void setAllowedLatenessMinutes(int allowedLatenessMinutes) { this.allowedLatenessMinutes = allowedLatenessMinutes; }

    public Integer getAllowedLatenessQ1Minutes() {
        return allowedLatenessQ1Minutes != null ? allowedLatenessQ1Minutes : allowedLatenessMinutes;
    }
    public void setAllowedLatenessQ1Minutes(Integer allowedLatenessQ1Minutes) {
        this.allowedLatenessQ1Minutes = allowedLatenessQ1Minutes;
    }

    public Integer getAllowedLatenessQ2_1hMinutes() {
        return allowedLatenessQ2_1hMinutes != null ? allowedLatenessQ2_1hMinutes : allowedLatenessMinutes;
    }
    public void setAllowedLatenessQ2_1hMinutes(Integer allowedLatenessQ2_1hMinutes) {
        this.allowedLatenessQ2_1hMinutes = allowedLatenessQ2_1hMinutes;
    }

    public Integer getAllowedLatenessQ2_6hMinutes() {
        return allowedLatenessQ2_6hMinutes != null ? allowedLatenessQ2_6hMinutes : allowedLatenessMinutes;
    }
    public void setAllowedLatenessQ2_6hMinutes(Integer allowedLatenessQ2_6hMinutes) {
        this.allowedLatenessQ2_6hMinutes = allowedLatenessQ2_6hMinutes;
    }

    public Integer getAllowedLatenessQ2_globalMinutes() {
        return allowedLatenessQ2_globalMinutes != null ? allowedLatenessQ2_globalMinutes : allowedLatenessMinutes;
    }
    public void setAllowedLatenessQ2_globalMinutes(Integer allowedLatenessQ2_globalMinutes) {
        this.allowedLatenessQ2_globalMinutes = allowedLatenessQ2_globalMinutes;
    }

    public Integer getAllowedLatenessQ3_1dMinutes() {
        return allowedLatenessQ3_1dMinutes != null ? allowedLatenessQ3_1dMinutes : allowedLatenessMinutes;
    }
    public void setAllowedLatenessQ3_1dMinutes(Integer allowedLatenessQ3_1dMinutes) {
        this.allowedLatenessQ3_1dMinutes = allowedLatenessQ3_1dMinutes;
    }

    public Integer getAllowedLatenessQ3_7dMinutes() {
        return allowedLatenessQ3_7dMinutes != null ? allowedLatenessQ3_7dMinutes : allowedLatenessMinutes;
    }
    public void setAllowedLatenessQ3_7dMinutes(Integer allowedLatenessQ3_7dMinutes) {
        this.allowedLatenessQ3_7dMinutes = allowedLatenessQ3_7dMinutes;
    }

    public int getWatermarkIdlenessMinutes() { return watermarkIdlenessMinutes; }
    public void setWatermarkIdlenessMinutes(int watermarkIdlenessMinutes) { this.watermarkIdlenessMinutes = watermarkIdlenessMinutes; }

    public CheckpointConfig getCheckpoint() { return checkpoint; }
    public void setCheckpoint(CheckpointConfig checkpoint) { this.checkpoint = checkpoint; }

    public int getMaxParallelism() { return maxParallelism; }
    public void setMaxParallelism(int maxParallelism) { this.maxParallelism = maxParallelism; }
}
