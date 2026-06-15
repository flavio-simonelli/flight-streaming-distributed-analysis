package it.uniroma2.sae.config;

/**
 * InfluxDB-specific configuration properties.
 */
public class InfluxDBConfig {
    private String url;
    private String token;
    private String org;
    private String bucket;

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getToken() {
        String prop = System.getProperty("INFLUXDB_TOKEN");
        if (prop != null) return prop;
        String env = System.getenv("INFLUXDB_TOKEN");
        if (env != null) return env;
        return token;
    }
    public void setToken(String token) { this.token = token; }

    public String getOrg() {
        String prop = System.getProperty("INFLUXDB_ORG");
        if (prop != null) return prop;
        String env = System.getenv("INFLUXDB_ORG");
        if (env != null) return env;
        return org;
    }
    public void setOrg(String org) { this.org = org; }

    public String getBucket() {
        String prop = System.getProperty("INFLUXDB_BUCKET");
        if (prop != null) return prop;
        String env = System.getenv("INFLUXDB_BUCKET");
        if (env != null) return env;
        return bucket;
    }
    public void setBucket(String bucket) { this.bucket = bucket; }
}
