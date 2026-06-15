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

    public String getToken() { return token; }
    public void setToken(String token) { this.token = token; }

    public String getOrg() { return org; }
    public void setOrg(String org) { this.org = org; }

    public String getBucket() { return bucket; }
    public void setBucket(String bucket) { this.bucket = bucket; }
}
