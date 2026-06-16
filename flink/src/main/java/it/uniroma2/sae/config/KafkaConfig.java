package it.uniroma2.sae.config;

/**
 * Kafka-specific configuration properties.
 */
public class KafkaConfig {
    private String host;
    private Integer port;
    private Integer internalPort;
    private Integer externalPort;
    private String inputTopic;
    private String sinkTopic;
    private String groupId;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() {
        if (port != null) return port;
        if (externalPort != null) return externalPort;
        return internalPort;
    }
    public void setPort(int port) { this.port = port; }

    public Integer getInternalPort() { return internalPort; }
    public void setInternalPort(Integer internalPort) { this.internalPort = internalPort; }

    public Integer getExternalPort() { return externalPort; }
    public void setExternalPort(Integer externalPort) { this.externalPort = externalPort; }

    public String getInputTopic() { return inputTopic; }
    public void setTopic(String inputTopic) { this.inputTopic = inputTopic; }

    public String getSinkTopic() { return sinkTopic; }
    public void setSinkTopic(String sinkTopic) { this.sinkTopic = sinkTopic; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }
}
