package it.uniroma2.sae.config;

/**
 * Flink-specific configuration properties.
 */
public class FlinkConfig {
    private String host;
    private int port;
    private int watermarkDelayMinutes = 10;
    private int allowedLatenessMinutes = 5;
    private int watermarkIdlenessMinutes = 2;
    private boolean checkpointingEnabled = true;
    private long checkpointIntervalMillis = 60_000L;
    private long minPauseBetweenCheckpointsMillis = 30_000L;
    private long checkpointTimeoutMillis = 120_000L;
    private int globalWindowTriggerHours = 1;

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

    public int getWatermarkIdlenessMinutes() { return watermarkIdlenessMinutes; }
    public void setWatermarkIdlenessMinutes(int watermarkIdlenessMinutes) { this.watermarkIdlenessMinutes = watermarkIdlenessMinutes; }

    public boolean isCheckpointingEnabled() { return checkpointingEnabled; }
    public void setCheckpointingEnabled(boolean checkpointingEnabled) { this.checkpointingEnabled = checkpointingEnabled; }

    public long getCheckpointIntervalMillis() { return checkpointIntervalMillis; }
    public void setCheckpointIntervalMillis(long checkpointIntervalMillis) { this.checkpointIntervalMillis = checkpointIntervalMillis; }

    public long getMinPauseBetweenCheckpointsMillis() { return minPauseBetweenCheckpointsMillis; }
    public void setMinPauseBetweenCheckpointsMillis(long minPauseBetweenCheckpointsMillis) { this.minPauseBetweenCheckpointsMillis = minPauseBetweenCheckpointsMillis; }

    public long getCheckpointTimeoutMillis() { return checkpointTimeoutMillis; }
    public void setCheckpointTimeoutMillis(long checkpointTimeoutMillis) { this.checkpointTimeoutMillis = checkpointTimeoutMillis; }
}
